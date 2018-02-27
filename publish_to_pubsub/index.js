const PubSub = require('@google-cloud/pubsub');
const typeOf = require('typeof');
exports.cf_presence_pub = (req, res) => {
    if (req.body === undefined) {       
        res.status(400).send(req.body);
    } else {
        console.log(req.body.proximity);
      var timestamp = new Date().getTime();
      	//console.log(req.body.timespan);
        var sk = [{
            "trackingId": req.body.trackingId,
            "proximity": req.body.proximity,
          	"timestamp": timestamp,
          	"status" : req.body.status
        }];      
        var skj=JSON.stringify(sk);
        console.log(skj); 
      publishMessage("projects/employeetracking-195305/topics/employee_job_event",skj);
    }
};

function publishMessage(topicName, data) {
    const pubsub = PubSub();
    const topic = pubsub.topic(topicName);
    const publisher = topic.publisher();
    const dataBuffer = Buffer.from(data);
    return publisher.publish(dataBuffer).then((results) => {
        const messageId = results[0];
        console.log(`Message ${messageId} published.`);
        return messageId;
        res.status(200).end();
    });
}