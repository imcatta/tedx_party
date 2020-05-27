const mongoose = require('mongoose');

const ReviewSchema = new mongoose.Schema({
  title: {
    type: String,
    required: false
  },
  content: {
    type: String,
    required: false
  },
  rate: {
    type: Number,
    required: true,
    min: 1,
    max: 5
  },
  username: {
    type: String,
    required: true
  },
  idTalk: {
    type: String,
    reuired: true
  }
}, { collection: 'reviews' });

module.exports = mongoose.model('review', ReviewSchema);