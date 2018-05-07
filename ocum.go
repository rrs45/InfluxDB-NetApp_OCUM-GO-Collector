package main

import (
  "database/sql"
  _ "github.com/go-sql-driver/mysql"
  "github.com/influxdata/influxdb/client/v2"
  "log"
  "fmt"
  "os"
  "time"
  )

const layout = "2006-01-02 MST"
var (
  d, cluster,aggr string
  used_tb,avail_tb, total_tb, used_percent int
  )
  
func influxDBClient() client.Client {
  c, err := client.NewHTTPClient(client.HTTPConfig{
    Addr: "https://<<host>>:8086",
    Username: "XXX",
    Password: "XXXX",
    InsecureSkipVerify: true,
  })
  if err!= nil {
    log.Fatalln("Error: ",err)
  }
  return c
}


func main() {
  log.SetOutput(os.Stdout)
  PST, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
    		fmt.Println(err)
    		return
	}
  c := influxDBClient()
  bp, err := client.NewBatchPoints(client.BatchPointsConfig{
    Database: "netapp_capacity",
    Precision: "s",
  })
  if err != nil {
    log.Fatalln("Error: ", err)
  }
  
  
  db1, err := sql.Open("mysql", "user:pwd@tcp(<<ip>>:3306)/netapp_model_view")
  db2, err := sql.Open("mysql", "suser:pwd@tcp(<<ip>>:3306)/netapp_model_view")
  dbs := []*sql.DB{db1,db2}
  for _, db := range dbs  { 
  if err!= nil {
    log.Fatal(err)
    }
    defer db.Close()
    err = db.Ping()
    if err != nil {
      fmt.Println("connected")
        }
        
    rows, err := db.Query(`SELECT       curdate() as Date, cluster.name as Cluster, aggregate.name AS Aggregate, 
            ROUND(aggregate.sizeUsed / (1024 * 1024 * 1024 * 1024), 0) AS UsedTB, ROUND(aggregate.sizeAvail / (1024 * 1024 * 1024 * 1024), 0) AS AvailTB,
                         ROUND(aggregate.sizeTotal / (1024 * 1024 * 1024 * 1024), 0) AS TotalTB, aggregate.sizeUsedPercent as UsedPercent
FROM            netapp_model_view.aggregate, netapp_model_view.cluster
WHERE        aggregate.clusterId = cluster.objid AND (NOT (aggregate.name REGEXP "aggr0"))`)
    if err != nil {
      log.Fatal(err)
    }
    
    defer rows.Close()
    for rows.Next() {
      err := rows.Scan(&d, &cluster,&aggr,&used_tb, &avail_tb,&total_tb, &used_percent)
      if err != nil {
        log.Fatal(err)
      }
      t, err := time.ParseInLocation(layout, fmt.Sprint(d," PST"), PST)
      //t,_ := time.Parse(layout,d)
      if err != nil {
        log.Fatal(err)
      }
      tags := map[string]string{
        "cluster": cluster,
        "aggregate": aggr,
         }
         
      fields := map[string]interface{}{
        "used_tb": used_tb,
        "available_tb": avail_tb,
        "total_tb": total_tb,
        "used_percent": used_percent,
      }
      point, err := client.NewPoint(
        "netapp_capacity",
        tags,
        fields,
        t.UTC(),
        )
      if err != nil {
                log.Fatalln("Error: ", err)
            }
    //fmt.Println(t.UTC(),d,cluster, aggr, used_tb, avail_tb, total_tb, used_percent)
    bp.AddPoint(point)
    }
  err = c.Write(bp)
  if err != nil {
    log.Fatal(err)
  }
}
}
