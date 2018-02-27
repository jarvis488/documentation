const BigQuery = require('@google-cloud/bigquery');
const PubSub = require('@google-cloud/pubsub');
const Spanner = require('@google-cloud/spanner');
const spanner = Spanner({
    projectId: 'employeetracking-195305',
});
const instanceId = 'locationtrackingproject';
const databaseId = 'kontakio';

exports.cf_presence_to_emp_job_event = (event, callback) => {

    const instance = spanner.instance(instanceId);
    const database = instance.database(databaseId);
    //subscribe from pubsub subscription
    function ps_to_spanner(subscriptionName) {
        const pubsub = PubSub();
        const subscription = pubsub.subscription(subscriptionName);
        let messageCount = 0;
        const messageHandler = (message) => {
            console.log(`Received message ${message.id}:`);
            console.log(`\tData: ${message.data}`);
            push_to_spanner(`${message.data}`);
            messageCount += 1;
            message.ack();
        };
        subscription.on(`message`, messageHandler);
        console.log(`${messageCount} message(s) received.`);
    }
    //call pubsub subscription
    ps_to_spanner('projects/employeetracking-195305/subscriptions/employee_job_event');

    //function to store temp data in spanner
    function push_to_spanner(message) {

        const rows = JSON.parse(message);
        console.log(JSON.parse(message));
        console.log(rows[0].trackingId)
        console.log(rows[0].status)
        console.log(rows[0].proximity)
        console.log(rows[0].timestamp)

        //check for status of trigger from kontakt cloud ie. detected or lost
        if (rows[0].status == 'detected') {

            // query timestamp from spanner punchintemp table according to emp_id

            const query = {

                sql: "SELECT * FROM PunchInTemp where emp_id='" + rows[0].trackingId + "'"
            };

            database.run(query).then(results => {

                console.log("Debugging1");

                // check for punchintemp contains data or it is null

                if (results[0].length == 0) {

                    //insert checkin record in punchintemp

                    const insertInTable = database.table('PunchInTemp');

                    insertInTable.insert([{
                        emp_id: "" + rows[0].trackingId + "",
                        check_in: "" + rows[0].timestamp + "",
                        check_out: '0',
                        Difference: '0',
                    }]).then(() => {
                        console.log('Inserted data.');
                    }).catch(err => {
                        console.error('ERROR:', err);
                    })
                } else { //parse object of punchintemp
                    var r = results[0][0].toJSON();
                    //check on checkout  for previous checkin

                    if (r.check_out == 0) {
                        console.log("Multiple check in without checkout");

                    } else if (r.check_out !== 0) {
                        var checkin_time = rows[0].timestamp;
                        console.log("variable" + typeof rows[0].timestamp)
                        var check_in_time_convert = new Date(checkin_time);
                        console.log(typeof check_in_time_convert);
                        console.log("checkintimeconvert" + check_in_time_convert);
                        var checkout = parseInt(r.check_out);
                        console.log("checkout" + typeof checkout);
                        var check_out_time_convert = new Date(checkout);
                        console.log(typeof check_out_time_convert);
                        console.log("checkoutimeconvert" + check_out_time_convert);
                        var timeDiff = Math.abs(check_in_time_convert.getTime() - check_out_time_convert.getTime());
                        console.log(typeof timeDiff);
                        console.log("timedifference is" + timeDiff);
                        var diffmins = Math.ceil(timeDiff / (60 * 1000))
                        console.log("Diffference in mind" + diffmins + typeof diffmins);
                        var dbcheck = parseInt(r.check_in);
                        var dbcheckin=new Date(dbcheck);
                      console.log("dbcheckin"+dbcheckin);
                        //check for punchin and punckout diffeence is greater than 2 or not 

                        if (diffmins > 2) {
                            //if diffrence is more than 2 minute then insert record in bigquery "employees_job_time_segment" table
                            console.log("inserting the data the diffmins"+diffmins+"checkin"+check_in_time_convert+"checkout"+check_out_time_convert );
                            //update value to
                            const bigquery = new BigQuery({
                                projectId: 'employeetracking-195305',
                            });
                          
                          const selectOrderEmp = "SELECT customer_order_id, order_line_number FROM KontaktIO.order_emp_assignment WHERE assigned_emp_id='"+rows[0].trackingId.toUpperCase()+"'";
							const options = {query: selectOrderEmp, useLegacySql: false,};
							bigquery.query(options).then(results => {
								
								orderid=results[0][0].customer_order_id;
								console.log(orderid);
								lineid=results[0][0].order_line_number;
								console.log(lineid);
                          
                          
                            const queryorderempassgn = "INSERT INTO KontaktIO.employees_job_time_segment (customer_order_id,order_line_number,emp_id,segment_start_time,segment_end_time,segment_elapsed_time_minutes) VALUES('"+orderid+"','"+lineid+"','" + rows[0].trackingId + "','" + dbcheckin + "','" + check_out_time_convert + "','" + r.Difference + "')";
                            const options = {
                                query: queryorderempassgn,
                                useLegacySql: false,
                            };
                            bigquery.query(options).then(results => {
                                console.log('data inserted...')
                            }).catch(err => {
                                console.error('ERROR:', err);
                            });
                              }).catch(err => {
                                console.error('ERROR:', err);
                            });


                            //const PunchInTemp = database.table('PunchInTemp');

                            const albumsTable = database.table('PunchInTemp');

                            albumsTable
                                .update([{
                                    emp_id: "" + rows[0].trackingId + "",
                                    check_in: "" + rows[0].timestamp + "",
                                    check_out: '0',
                                    Difference: '0'
                                }, ])
                                .then(() => {
                                    console.log('Updated data.');
                                })
                                .catch(err => {
                                    console.error('ERROR:', err);
                                });
                        } else {
                            //console.log("Checkout detected with difference in 1 min");
                            const albumsTable = database.table('PunchInTemp');

                            albumsTable
                                .update([{
                                    emp_id: "" + rows[0].trackingId + "",
                                    //check_in: "" + r.check_in + "",
                                    check_out: "" + '0' + "",
                                    Difference: "" + '0' + ""
                                }, ])
                                .then(() => {
                                    console.log('Updated data.');
                                })
                                .catch(err => {
                                    console.error('ERROR:', err);
                                });
                        }
                    }
                }
            }).catch(err => {
                console.error('ERROR:', err);
            });
            console.log("Debugging3");
            //console.log(run);
        } else {


            const query = {

                sql: "SELECT * FROM PunchInTemp where emp_id='" + rows[0].trackingId + "'"
            };

            database.run(query).then(results => {

                var r = results[0][0].toJSON();

                if (r.check_out == 0) {


                    console.log("detected the checkout");

                    console.log("calculating the diffeerence between checkin and checkout");




                    var checkout_time = rows[0].timestamp;
                    console.log("variable" + typeof rows[0].timestamp)
                    var check_out_time_convert = new Date(checkout_time);
                    console.log(typeof check_out_time_convert);
                    console.log("checkintimeconvert" + check_out_time_convert);
                    var checkin = parseInt(r.check_in);
                    console.log("checkout" + typeof checkin);
                    var check_in_time_convert = new Date(checkin);
                    console.log(typeof check_in_time_convert);
                    console.log("checkoutimeconvert" + check_out_time_convert);
                    var timeDiff = Math.abs(check_out_time_convert.getTime() - check_in_time_convert.getTime());
                    console.log(typeof timeDiff);
                    console.log("timedifference is" + timeDiff);
                    var diffmins = Math.ceil(timeDiff / (60 * 1000))
                    console.log("Diffference in mins" + diffmins);


                    if (diffmins > 2) {


                        const albumsTable = database.table('PunchInTemp');

                        albumsTable
                            .update([{
                                emp_id: "" + rows[0].trackingId + "",
                                check_in: "" + r.check_in + "",
                                check_out: "" + rows[0].timestamp + "",
                                Difference: "" + diffmins + ""
                            }, ])
                            .then(() => {
                                console.log('Updated data.');
                            })
                            .catch(err => {
                                console.error('ERROR:', err);
                            });




                    }



                } else {

                    console.log("multiple checkout without checkin");

                }




            }).catch(err => {
                console.error('ERROR:', err);
            });



        }




    }
}