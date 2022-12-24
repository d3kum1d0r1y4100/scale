package main

import (
//	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
//	"github.com/hakluke/hakscale"
//	"hakscale/packages"
//	"hakscale/pop"
	"github.com/go-redis/redis"
	"gopkg.in/yaml.v2"
        "bufio"
  //      "fmt"
    //    "log"
//        "os"
        "strconv"
        "strings"
        "sync"
        "time"
	"context"
//	"fmt"
//	"log"
	"os/exec"
//	"strconv"
//	"strings"
//	"sync"
//	"time"

      //  "github.com/go-redis/redis"
        "github.com/google/uuid"
)

//Config struct holds all configuration data that comes from config.yml or environment variables
type Config struct {
	Redis struct {
		Host     string `yaml:"host" envconfig:"REDIS_HOST"`
		Port     string `yaml:"port" envconfig:"REDIS_PORT"`
		Password string `yaml:"password" envconfig:"REDIS_PASSWORD"`
	} `yaml:"redis"`
}

// Global variables
var config *Config
var redisClient *redis.Client


func pushIt(command string, queue string, parametersString string, test bool, timeout int, verbose bool) {

	var split []string
	var filenames []string
	var placeholders []string

	// split the parameters input into a slice of placeholder:filename pairs
	parameters := strings.Split(parametersString, ",")

	// separate the placeholder from the filename and store them in two slices for processing
	for _, p := range parameters {
		split = strings.Split(p, ":")
		placeholders = append(placeholders, split[0])
		filenames = append(filenames, split[1])
	}

	// get a slice of lines for each filename
	var fileSlices [][]string
	for _, filename := range filenames {
		newFileLines, err := readLines(filename)
		if err != nil {
			log.Fatal(err)
		}
		fileSlices = append(fileSlices, newFileLines)
	}

	// for each file, figure out how many lines there are. Store lengths of each file in a slice called lengths, and do the same for originalLengths
	var lengths []int
	var originalLengths []int

	for i := range fileSlices {
		length := len(fileSlices[i])
		lengths = append(lengths, length)
		originalLengths = append(originalLengths, length)
	}

	var wg sync.WaitGroup

	// create a rendom queue name for sending back data
	queueID := uuid.New().String()

	// get the results back and print them
	go printResults(queueID, &wg, verbose)

	// this is the recursive function that generates all of the command combinations
	loopThrough(fileSlices, placeholders, command, lengths, originalLengths, test, &wg, queueID, timeout, queue)

	// wait for all results to be returned and printed before exiting
	wg.Wait()
}
func printResults(queueID string, wg *sync.WaitGroup, verbose bool) {
	for {
		result, err := redisClient.RPop(queueID).Result()

		switch {
		case err == redis.Nil: // the queue doesn't exist, there's no output to print yet
			if verbose {
				log.Println("Awaiting output:", err)
			}
			time.Sleep(1 * time.Second)
		case err != nil: // there was an actual error trying to grab the data
			log.Println("Redis error:", err)
		case result == "": // the command returned no output, don't bother printing a blank line
			wg.Done()
		default: // we got output, print it!
			fmt.Println(result)
			wg.Done()
		}
	}
}

// checks if all elements of a slice are equal to comparator
func checkIfAll(array []int, comparator int) bool {
	for _, i := range array {
		if i != comparator {
			return false
		}
	}
	return true
}

// Recursive function that takes slices of strings, and prints every combination of lines in each file
func loopThrough(fileSlices [][]string, placeholders []string, command string, lengths []int, originalLengths []int, test bool, wg *sync.WaitGroup, queueID string, timeout int, queue string) {
	if checkIfAll(lengths, 0) {
		return
	}
	line := command

	for i, fileSlice := range fileSlices {

		// if the RHS number is 0, finish
		if lengths[len(lengths)-1] == 0 {
			return
		}

		// replace the placeholders in the line
		line = strings.ReplaceAll(line, "_"+placeholders[i]+"_", fileSlice[lengths[i]-1])
	}

	// if -test is specified, just print the commands, otherwise push them to redis
	if test {
		fmt.Println(line)
	} else {
		// using :::_::: as a separator between the queueID, timeout and command
		redisClient.LPush(queue, queueID+":::_:::"+strconv.Itoa(timeout)+":::_:::"+line)
		wg.Add(1)
	}

	for i := range lengths {
		// if our current number is a 0, decrement the number directly to the right, then set this back to the original
		if lengths[i] == 1 && len(lengths) != i+1 {
			lengths[i+1] = lengths[i+1] - 1
			lengths[i] = originalLengths[i]
			break
		} else {
			lengths[i] = lengths[i] - 1
			break
		}
	}

	loopThrough(fileSlices, placeholders, command, lengths, originalLengths, test, wg, queueID, timeout, queue)
}

// readLines reads a whole file into memory and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func popIt(threads int, queue string, verbose bool) {

	var wg sync.WaitGroup

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			doWork(&wg, queue, verbose)
		}()
	}

	wg.Wait()
}

func doWork(wg *sync.WaitGroup, queue string, verbose bool) {
	defer wg.Done()
	for {
		result, err := redisClient.RPop(queue).Result()
		if err != nil {
			if verbose {
				log.Println("Polling for jobs.")
			}
			time.Sleep(1 * time.Second)
		} else {
			shellexec(result, verbose)
		}
	}
}

func shellexec(command string, verbose bool) {
	ctx := context.Background()
	split := strings.Split(command, ":::_:::")
	queue := split[0] // this is a randomly generated uuid to be used as a queue name for returning the output
	timeout, err := strconv.Atoi(split[1])
	if err != nil {
		log.Println(err)
		redisClient.LPush(queue, err.Error) // push error to the queue
		return
	}
	command = split[2]

	if verbose {
		log.Println("Running command:", command)
	}

	commandctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeout)) // add context to include a timeout
	defer cancel()                                                                                      // The cancel should be deferred so resources are cleaned up
	out, err := exec.CommandContext(commandctx, "/bin/bash", "-c", command).Output()
	if commandctx.Err() == context.DeadlineExceeded {
		writeToQueueAndPrint(ctx, command, queue, []byte("Command timed out.\n")) // push error to the queue
	} else if err != nil {
		writeToQueueAndPrint(ctx, command, queue, []byte(err.Error())) // push error to the queue
	} else {
		writeToQueueAndPrint(ctx, command, queue, out)
	}
}

// writeToQueueAndPrint will print the command output and then write it to the redis queue
func writeToQueueAndPrint(ctx context.Context, command string, queue string, output []byte) {
	log.Println("Output for command:", command)
	fmt.Println(string(output))      // print command output
	redisClient.LPush(queue, output) // push the command output to the queue
}

func main() {

	// if less than 2 arguments, error out
	if len(os.Args) < 2 {
		fmt.Println("Error: Subcommand missing or incorrect.\n\nHint: you can push jobs to the queue with:\n\nhakscale push -p \"param1:./file1.txt,param2:./file2.txt\" -c \"nmap -A _param1_ _param2_\" -t 20\n\nOr you can pop them from the queue and execute them with:\n\nhakscale pop -q nmap -t 20\n\nFor full usage instructions, see github.com/hakluke/hakscale")
		return
	}

	// load config file
	f, err := os.Open(os.Getenv("HOME") + "/.config/haktools/hakscale-config.yml")
	if err != nil {
		fmt.Println("Error opening config file:", err)
	}
	defer f.Close()

	// parse the config file
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&config)
	if err != nil {
		fmt.Println("Error decoding config.yml", err)
		return
	}

	// Connect to redis server
	redisClient = redis.NewClient(&redis.Options{
		Addr:      config.Redis.Host + ":" + config.Redis.Port,
		Password:  config.Redis.Password,
		DB:        0,                                     // redis databases are deprecated so we will just use the default
//		TLSConfig: &tls.Config{InsecureSkipVerify: true}, // needed for the standard DO redis
	})

	// Check redis server connection
	_, err = redisClient.Ping().Result()
	if err != nil {
		fmt.Println("Unable to connect to specified Redis server:", err)
		os.Exit(1)
	}

	switch os.Args[1] {

	case "push":
		flagSet := flag.NewFlagSet("hakscale push", flag.ExitOnError)
		verbose := flagSet.Bool("v", false, "verbose mode")
		command := flagSet.String("c", "", "the command you wish to scale, including placeholders")
		queue := flagSet.String("q", "cmd", "the name of the queue that you would like to push jobs to")
		parametersString := flagSet.String("p", "", "the placeholders and files being used")
		test := flagSet.Bool("test", false, "print the commands to terminal, don't actually push them to redis")
		timeout := flagSet.Int("t", 0, "timeout for the commands (in seconds)")
		flagSet.Parse(os.Args[2:])
		if *timeout == 0 {
			log.Fatal("You must specify a timeout to avoid leaving your workers endlessly working. Hint: -t <seconds>")
		}
		pushIt(*command, *queue, *parametersString, *test, *timeout, *verbose)
	case "pop":
		flagSet := flag.NewFlagSet("hakscale pop", flag.ExitOnError)
		verbose := flagSet.Bool("v", false, "verbose mode")
		queue := flagSet.String("q", "cmd", "the name of the queue that you would like to pop jobs from")
		threads := flagSet.Int("t", 5, "number of threads")
		flagSet.Parse(os.Args[2:])
		popIt(*threads, *queue, *verbose)

	// no valid subcommand found - default to showing a message and exiting
	default:
		fmt.Println("Error: Subcommand missing or incorrect. Hint, you can push jobs to the queue with:\n\nhakscale push -p \"param1:./file1.txt,param2:./file2.txt\" -c \"nmap -A _param1_ _param2_\" -t 20\n\nOr you can pop them from the queue and execute them with:\n\nhakscale pop -q nmap -t 20")
		os.Exit(1)
	}
}
