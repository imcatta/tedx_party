const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id:String,
    title: String,
    url: String,
    details: String,
    main_speaker: String,
    posted: String,
    tags: Array,
    watch_next_ids:Array,
    reviews:Number,
    average_reviews:Number
}, { collection: 'tedz_data' });

module.exports = mongoose.model('talk', talk_schema);