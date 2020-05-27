// CONNECTION TO DB

const mongoose = require('mongoose');
mongoose.Promise = global.Promise;
let isConnected;

module.exports = () => {
  if (isConnected) {
    console.log('=> using existing database connection');
    return Promise.resolve();
  }

  console.log('=> using new database connection');
  return mongoose
    .connect(process.env.DB, {
      dbName: 'unibg_tedx',
      useNewUrlParser: true,
      useUnifiedTopology: true,
    })
    .then((db) => {
      isConnected = db.connections[0].readyState;
    });
};
