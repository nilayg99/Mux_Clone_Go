package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/joho/godotenv"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func envLoad() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
	}
	sqsURL := os.Getenv("sqs_url")
	// Provide AWS credentials
	creds := credentials.NewStaticCredentials(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"", // If you have a session token, provide it here, otherwise leave it empty
	)
	sessionCreate(sqsURL, creds)

}
func sessionCreate(sqsURL string, creds *credentials.Credentials) {
	// Initialize AWS session
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"), // Change to your region
		Credentials: creds,
	})
	if err != nil {
		fmt.Println("Error creating AWS session:", err)
	}

	// Create SQS service client
	svc := sqs.New(sess)
	for {
		receiveMessages(svc, sqsURL)
	}

}
func sendToPythonScript(message string) error {
	cmd := exec.Command("python", "video_processing/Main.py", message)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error running Python script: %v, output: %s", err, string(output))
	}
	fmt.Println(string(output))
	return nil

}
func receiveMessages(svc *sqs.SQS, sqsURL string) {
	resp, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(sqsURL),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(10),
	})

	if err != nil {
		fmt.Println("Error receiving messages:", err)
		return
	}

	for _, message := range resp.Messages {
		//fmt.Printf("Message: %s\n", aws.StringValue(message.Body))
		err := sendToPythonScript(aws.StringValue(message.Body))
		if err != nil {
			fmt.Println("Error sending message to Python script:", err)
		}

		// ... processing and deleting messages

	}
	if len(resp.Messages) == 0 {
		fmt.Println("No messages found.")
		return
	}
}

func main() {
	envLoad()

}
