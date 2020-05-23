const connect_to_db = require('./db');

// GET BY TALK HANDLER

const watch_next = require('./watch_next');

module.exports.get_next_talks = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if(!body.id_video) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the next talks. No id is provided.'
        })
    }
    
    
    connect_to_db().then(() => {
        console.log('=> get next talks');
        watch_next.aggregate(
            [
    {
        '$lookup': {
            'from': 'tedz_data', 
            'localField': 'watch_next_ids', 
            'foreignField': '_id', 
            'as': 'watch_next_info'
        }
    }, {
        '$match': {
            '_id': body.id_video
        }
    }, {
        '$project': {
            '_id': 1, 
            'watch_next_info': 1
        }
    }
])
            .then(watch_next => {
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(watch_next)
                    })
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch next talks.'
                })
            );
    });
};