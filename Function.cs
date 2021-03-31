using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.Lambda.Serialization.Json;
using Amazon.Lambda.Core;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;
using Amazon.DynamoDBv2.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace TriggerExample
{
    [Serializable]
    public class Item
    {
        public string itemId; //primary key in DB
        public string description;
        public int rating;
        public string type;
        public string company;
        //public string lastInstanceOfWord="x";
    }
    public class Type
    {
        public string type;
        public string count;
        public string total;
        public string average;
    }
    public class Function
    {
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        public async Task<List<Item>> FunctionHandler(DynamoDBEvent input, ILambdaContext context)
        {
            Table table = Table.LoadTable(client, "RatingsByType");
            List<Item> items = new List<Item>();
            List<DynamoDBEvent.DynamodbStreamRecord> records = (List<DynamoDBEvent.DynamodbStreamRecord>)input.Records;            
            #region single record
            if (records.Count > 0)
            {
                DynamoDBEvent.DynamodbStreamRecord record = records[0];
                if (record.EventName.Equals("INSERT"))
                {
                    Document doc = Document.FromAttributeMap(record.Dynamodb.NewImage);
                    Item myItem = JsonConvert.DeserializeObject<Item>(doc.ToJson());                    
                    var request = new UpdateItemRequest
                    {
                        TableName = "RatingsByType",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            { "type",new AttributeValue { S=myItem.type} }
                        },
                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        { 
                            {"count",new AttributeValueUpdate{Action="ADD",Value=new AttributeValue{N="1"} } },
                            {"total",new AttributeValueUpdate{Action="ADD",Value=new AttributeValue{N=myItem.rating.ToString() } } },
                            {"average",new AttributeValueUpdate{Action="PUT",Value=new AttributeValue{
                                N="0"} } },
                        },                        
                    };                    
                    Console.WriteLine(request.ReturnValues);
                    await client.UpdateItemAsync(request);
                    GetItemResponse res = await client.GetItemAsync("RatingsByType", new Dictionary<string, AttributeValue>{
                        {"type",new AttributeValue{S=myItem.type }}});
                    Type myType = JsonConvert.DeserializeObject<Type>(Document.FromAttributeMap(res.Item).ToJson());
                    Console.WriteLine(myType.count);
                    var aver = new UpdateItemRequest
                    {
                        TableName="RatingsByType",
                        Key=new Dictionary<string, AttributeValue> { { "type",new AttributeValue { S=myItem.type} } },
                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        {
                            {"average",new AttributeValueUpdate{Action="PUT",Value=new AttributeValue{
                                N=(int.Parse(myType.total)/int.Parse(myType.count)).ToString()
                            } } },
                        },
                    };
                    await client.UpdateItemAsync(aver);
                }
            }
            #endregion
            return items;
        }
    }
}
//{
//    "Records": [
//      {
//        "eventID": "1",
//      "eventName": "INSERT",
//      "eventVersion": "1.0",
//      "eventSource": "aws:dynamodb",
//      "awsRegion": "{region}",
//      "dynamodb": {
//            "Keys": {
//                "itemId": {
//                    "S": "asdf"
//                }
//            },
//        "NewImage": {
//                "description": {
//                    "S": "New item!"
//                },
//"rating":{ "N":"4"},
//"type":{ "S":"comad"},
//"company":{ "S":"a"},
//          "itemId": {
//                    "S": "asdf"
//          }
//            },
//        "SequenceNumber": "111",
//        "SizeBytes": 26,
//        "StreamViewType": "NEW_AND_OLD_IMAGES"
//      }
//      }
//  ]
//}