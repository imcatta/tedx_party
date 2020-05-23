const mongoose = require('mongoose');

const watch_next_schema = new mongoose.Schema({
    "_id": String,
    "watch_next_info":Array,
}, { collection: 'tedz_data' });

module.exports = mongoose.model('watch_next', watch_next_schema);