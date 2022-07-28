package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/sagar23sj/go-grpc/greet/greetpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {

	cc, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer cc.Close()

	c := greetpb.NewGreetServiceClient(cc)

	//Unary API function - Greet
	// Greet(c)

	// Server-Side Streaming function - GreetManyTimes
	// GreetManyTimes(c)

	//Client-Side Straming function - LongGreet
	// LongGreet(c)

	//bidirectional streaming function - GreetEveryone
	// GreetEveryone(c)

	//unary API with Deadline
	// GreetWithDeadline(c)

	//unary API with Error Handling
	GreetWithErrorHandling(c)
}

func Greet(c greetpb.GreetServiceClient) {

	fmt.Println("Starting to do a unary GRPC....")

	req := greetpb.GreetRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "John",
			LastName:  "Doe",
		},
	}

	resp, err := c.Greet(context.Background(), &req)
	if err != nil {
		log.Fatalf("error while calling greet grpc unary call: %v", err)
	}

	log.Printf("Response from Greet Unary Call : %v", resp.Result)

}

func GreetManyTimes(c greetpb.GreetServiceClient) {
	fmt.Println("Staring ServerSide GRPC streaming ....")

	req := greetpb.GreetManyTimesRequest{

		Greeting: &greetpb.Greeting{
			FirstName: "Michael",
			LastName:  "Jackson",
		},
	}

	respStream, err := c.GreetManyTimes(context.Background(), &req)
	if err != nil {
		log.Fatalf("error while calling GreetManyTimes server-side streaming grpc : %v", err)
	}

	for {
		msg, err := respStream.Recv()
		if err == io.EOF {
			//we have reached to the end of the file
			break
		}

		if err != nil {
			log.Fatalf("error while receving server stream : %v", err)
		}

		fmt.Println("Response From GreetManyTimes Server : ", msg.Result)
	}
}

func LongGreet(c greetpb.GreetServiceClient) {

	fmt.Println("Starting Client Side Streaming over GRPC ....")

	stream, err := c.LongGreet(context.Background())
	if err != nil {
		log.Fatalf("error occured while performing client-side streaming : %v", err)
	}

	requests := []*greetpb.LongGreetRequest{
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "James",
				LastName:  "Bond",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Tim",
				LastName:  "Cook",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Elon",
				LastName:  "Musk",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Sam",
				LastName:  "Rutherford",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Eoin",
				LastName:  "Morgan",
			},
		},
	}

	for _, req := range requests {
		fmt.Println("\nSending Request.... : ", req)
		stream.Send(req)
		time.Sleep(1 * time.Second)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error while receiving response from server : %v", err)
	}
	fmt.Println("\n****Response From Server : ", resp.GetResult())
}

func GreetEveryone(c greetpb.GreetServiceClient) {
	fmt.Println("Starting Bi-directional stream by calling GreetEveryone over GRPC......")

	requests := []*greetpb.GreetEveryoneRequest{
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "James",
				LastName:  "Bond",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Harry",
				LastName:  "Mitchel",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Tim",
				LastName:  "Cook",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Sam",
				LastName:  "Rutherford",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Eoin",
				LastName:  "Morgan",
			},
		},
	}

	stream, err := c.GreetEveryone(context.Background())
	if err != nil {
		log.Fatalf("error occured while performing client side streaming : %v", err)
	}

	//wait channel to block receiver
	waitchan := make(chan struct{})

	go func(requests []*greetpb.GreetEveryoneRequest) {
		for _, req := range requests {

			fmt.Println("\nSending Request..... : ", req.Greeting)
			err := stream.Send(req)
			if err != nil {
				log.Fatalf("error while sending request to GreetEveryone service : %v", err)
			}
			time.Sleep(1000 * time.Millisecond)
		}
		stream.CloseSend()
	}(requests)

	go func() {
		for {

			resp, err := stream.Recv()
			if err == io.EOF {
				close(waitchan)
				return
			}

			if err != nil {
				log.Fatalf("error receiving response from server : %v", err)
			}

			fmt.Printf("\nResponse From Server : %v", resp.GetResult())
		}
	}()

	//block until everything is finished
	<-waitchan
}

func GreetWithDeadline(c greetpb.GreetServiceClient) {
	fmt.Println("\nStarting unary streaming with deadline .......")

	// doGreetWithDeadline(c, 5*time.Second)

	doGreetWithDeadline(c, 2*time.Second)
}

func doGreetWithDeadline(c greetpb.GreetServiceClient, timeout time.Duration) {

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fmt.Printf("Staring to do a unary streaming with deadline of : %v", timeout)

	request := &greetpb.GreetWithDeadlineRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "John",
			LastName:  "Doe",
		},
	}

	resp, err := c.GreetWithDeadline(ctx, request)
	if err != nil {

		statusErr, ok := status.FromError(err)

		fmt.Printf("\nError code : %v", statusErr.Code())
		fmt.Printf("\nError Message : %v", statusErr.Message())
		if ok {
			if statusErr.Code() == codes.DeadlineExceeded {
				fmt.Println("\nOperation cannnot proceed further, Deadline Exceeded")
				return
			} else {
				log.Fatalf("unexpected error occured : %v", statusErr)
				return
			}
		} else {

			log.Fatalf("error occured while calling GreetWithDeadline on Server : %v", err)
			return
		}
	}

	fmt.Println("\nResponse From server : ", resp.GetResult())
}

func GreetWithErrorHandling(c greetpb.GreetServiceClient) {

	fmt.Println("Starting to do a unary GRPC with Error Handling....")

	req := greetpb.GreetWithErrorHandlingRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "John",
			LastName:  "John",
		},
	}

	resp, err := c.GreetWithErrorHandling(context.Background(), &req)
	if err != nil {
		respErr, ok := status.FromError(err)
		if ok {
			//actual error from GRPC (user error)
			fmt.Println("\nError Message : ", respErr.Message())
			fmt.Println("\nError Code : ", respErr.Code())

			if respErr.Code() == codes.InvalidArgument {
				fmt.Println("Probably sent same first name and last name!")
				return
			}
		} else {
			log.Fatalf("error calling GreetWithErrorHandling : %v", err)
			return
		}
	}

	log.Printf("Response from Greet Unary Call with Error Handling : %v", resp.Result)
}
