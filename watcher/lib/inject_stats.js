var _ = require('lodash');
var moment = require('moment');
var client = require('./client');

module.exports = function (fixture) {
  var indexPattern = fixture.indexPattern;
  var type = fixture.type;
  var data = fixture.data;
  var rawData = fixture.rawData;
  var startDate = fixture.startDate;
  var duration = fixture.duration;
  var dateField = fixture.dateField;
  var workingDate = moment.utc();
  var indices = [];
  var body = [];
  var fields;


  function createEntries(row) {
    var index = workingDate.format(indexPattern);
    var entry = { '@timestamp': workingDate.toISOString() };
    row.forEach(function (val, index) {
      _.set(entry, fields[index], val);
    });

    indices.push(index);

    body.push({
      index: {
        _index: index,
        _type: type
      }
    });
    body.push(entry);
  }

  if (rawData) {
    rawData.forEach(function (row) {
      var index = moment.utc(row[dateField]).format(indexPattern);
      var entry = {};
      _.each(row, function (val, key) {
        _.set(entry, key, val);
      });
      indices.push(index);
      body.push({
        index: {
          _index: index,
          _type: type
        }
      });
      body.push(entry);
    });
  } else {
    fields = data.shift();
    while(startDate <= workingDate) {
      data.forEach(createEntries);
      workingDate.subtract(duration);
    }
  }

  return client.deleteByQuery({
    index: _.unique(indices),
    ignoreUnavailable: true,
    allowNoIndices: true,
    q: '*'
  })
  .then(function (arg) {
    return client.bulk({
      body: body,
      refresh: true
    });
  });
};
