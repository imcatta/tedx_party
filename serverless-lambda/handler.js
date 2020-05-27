'use strict';

const db = require('./db');
const review = require('./models/Review');
const talk = require('./models/Talk');

module.exports.getByTag = (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  console.log('Received event:', JSON.stringify(event, null, 2));
  let body = {};
  if (event.body) {
    body = JSON.parse(event.body);
  }
  // set default
  if (!body.tag) {
    callback(null, {
      statusCode: 500,
      headers: { 'Content-Type': 'text/plain' },
      body: 'Could not fetch the talks. Tag is null.',
    });
  }

  if (!body.doc_per_page) {
    body.doc_per_page = 10;
  }
  if (!body.page) {
    body.page = 1;
  }

  connect_to_db().then(() => {
    console.log('=> get_all talks');
    talk
      .find({ tags: body.tag })
      .sort({ rateAverage: -1 }) // sort by rateAverage
      .skip(body.doc_per_page * body.page - body.doc_per_page)
      .limit(body.doc_per_page)
      .then((talks) => {
        callback(null, {
          statusCode: 200,
          body: JSON.stringify(talks),
        });
      })
      .catch((err) =>
        callback(null, {
          statusCode: err.statusCode || 500,
          headers: { 'Content-Type': 'text/plain' },
          body: 'Could not fetch the talks.',
        })
      );
  });
};

module.exports.getWatchNext = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  await db();

  return {
    statusCode: 200,
    body: JSON.stringify({ message: 'TODO' }),
  };
};

module.exports.getReviews = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  await db();

  return {
    statusCode: 200,
    body: JSON.stringify({ message: 'TODO' }),
  };
};

module.exports.publishReview = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  await db();

  let body = {};
  if (event.body) {
    body = JSON.parse(event.body);
  }

  // set default
  if (!body.id_talk) {
    callback(null, {
      statusCode: 400, // BAD REQUEST
      headers: { 'Content-Type': 'text/plain' },
      body: 'Could not review any talk. No id is provided.',
    });
  }
  if (!body.rate) {
    callback(null, {
      statusCode: 400,
      headers: { 'Content-Type': 'text/plain' },
      body: 'Could not review any talk. No rate is provided',
    });
  }
  if (!body.username) {
    callback(null, {
      statusCode: 400,
      headers: { 'Content-Type': 'text/plain' },
      body: 'Could not review any talk. No username provided',
    });
  }

  // If a review from the same user already exists then
  // update the existing one, else create a new review
  await review.update(
    {
      idTalk: body.id_talk,
      username: body.username,
    },
    {
      idTalk: body.id_talk,
      username: body.username,
      rate: body.rate,
      title: body.title,
      content: body.content,
    },
    { upsert: true }
  );

  // calculate the new rate average
  const agg = await review.aggregate([
    { $match: { idTalk: body.id_talk } },
    {
      $group: {
        _id: '$idTalk',
        rateAverage: { $avg: '$rate' },
      },
    },
  ]);

  // update the talk object with the new rateAverage
  talk.update({ _id: body.id_talk }, { rateAverage: agg.rateAverage });

  // return OK
  return {
    statusCode: 200,
    headers: { 'Content-Type': 'text/plain' },
    body: 'OK',
  };
};
