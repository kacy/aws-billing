package main

import (
    "fmt"
    "flag"
    "encoding/csv"
    "os"
    "log"
    "gopkg.in/mgo.v2"
)

func main() {
    host := flag.String("host", "mongo.test.com", "the url to your mongo host")
    port := flag.String("port", "27017", "the port to your mongo service")
    db   := flag.String("db", "aws_billing", "the database where we'll store everything")
    aws_csv  := flag.String("csv", "aws.csv", "the path to the aws csv file that you want to use")

    flag.Parse()

    file, err := os.Open(*aws_csv)

    if err != nil {
      fmt.Println(err)
      return
    }

    defer file.Close()

    reader := csv.NewReader(file)

    reader.FieldsPerRecord = -1

    rawCSVdata, err := reader.ReadAll()

    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }

    for _, each := range rawCSVdata {
        r := Record {
          InvoiceId: each[0],
          PayerAccountId: each[1],
          LinkedAccountId: each[2],
          RecordType: each[3],
          RecordId: each[4],
          ProductName: each[5],
          RateId: each[6],
          SubscriptionId: each[7],
          PricingPlanId: each[8],
          UsageType: each[9],
          Operation: each[10],
          AvailabilityZone: each[11],
          ReservedInstance: each[12],
          ItemDescription: each[13],
          UsageStartDate: each[14],
          UsageEndDate: each[15],
          UsageQuantity: each[16],
          Rate: each[17],
          Cost: each[18],
          ResourceId: each[19],
        }

        store(r, *host, *port, *db)
    }
}

type Record struct {
    InvoiceId string
    PayerAccountId string
    LinkedAccountId string
    RecordType string
    RecordId string
    ProductName string
    RateId string
    SubscriptionId string
    PricingPlanId string
    UsageType string
    Operation string
    AvailabilityZone string
    ReservedInstance string
    ItemDescription string
    UsageStartDate string
    UsageEndDate string
    UsageQuantity string
    Rate string
    Cost string
    ResourceId string
}

func store(r Record, host string, port string, db string) {
    connection_string := fmt.Sprintf("%s:%s", &host, &port)
    session, err := mgo.Dial(connection_string)
    if err != nil {
        panic(err)
    }
    defer session.Close()

    c := session.DB(db).C("aws_001")

    err = c.Insert(r)

    if err != nil {
        log.Fatal(err)
    }
}
