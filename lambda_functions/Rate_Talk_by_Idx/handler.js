const connect_to_db = require('./db');

// GET BY TALK HANDLER
const mongoose = require('mongoose');
const review = require('./Review');
const talk = require('./Talk');

module.exports.add_review = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if(!body.id_talk) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not review any talk. No id is provided.'
        })
    }
    if(!body.rate){
        callback(null,{
            statusCode:500,
            headers:{'Content-Type':'text/plain'},
            body: 'Could not review any talk. No rate is provided'
        })
    }
    if(!body.username){
        callback(null,{
            statusCode:500,
            headers:{'Content-Type':'text/plain'},
            body: 'Could not review any talk. No username provided'
        })
    }
     
    connect_to_db().then(() => {
        console.log('=> checking if already reviewed');
        review.find({username:body.username,idTalk:body.id_talk},function (err, revs) 
        {
            if (err)
                callback(null,{
                    statusCode:500,
                    headers:{'Content-Type':'text/plain'},
                    body: 'Error while checking if already reviewed the talk'+err
                })
            else if (revs)
                callback(null,{
                    statusCode:500,
                    headers:{'Content-Type':'text/plain'},
                    body: 'The user has already reviewed this talk'
                })
        });
        console.log("=> updating talk rate");
        talk.find({_id:body.id_talk},function(err,tlk)
        {
            if (err)
                callback(null,{
                statusCode:500,
                headers:{'Content-Type':'text/plain'},
                body: 'Error while updating average talk rate'+err
            })
            else
            {
                if (!tlk)
                    callback(null,{
                        statusCode:500,
                        headers:{'Content-Type':'text/plain'},
                        body: 'This talk doesn\'t exist'
                    })
            }
           if(!tlk.reviews || tlk.reviews==0 )
            {
                tlk.reviews=1;
                tlk.average_reviews=body.rate
            }
            else{
                tlk.reviews+=1;
                tlk.average_reviews=(tlk.average_reviews*(tlk.reviews-1)/tlk.reviews)+(body.rate/tlk.reviews)
            }
            try{
                tlk.save()
            }
            catch(ex)
            {
                callback(null,{
                    statusCode:500,
                    headers:{'Content-Type':'text/plain'},
                    body: 'update review average rate failed'+ex
                })
            }
        });

        console.log('=> review talk');
        review.create({idTalk:body.id_talk,rate:body.rate,title:body.title,content:body.content,username:body.username},function (err, rev) {
            if (err) 
                callback(null,{
                    statusCode:500,
                    headers:{'Content-Type':'text/plain'},
                    body: 'Error while saving review'
                })
            callback(null,{
                statusCode:200,
                headers:{'Content-Type':'text/plain'},
                body: 'Review saved.'
            })
          });
    });
};