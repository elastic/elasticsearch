/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.compute {

    requires org.apache.lucene.analysis.common;
    requires org.apache.lucene.core;
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.compute.ann;
    requires org.elasticsearch.xcontent;
    // required due to dependency on org.elasticsearch.common.util.concurrent.AbstractAsyncTask
    requires org.apache.logging.log4j;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.ml;
    requires org.elasticsearch.tdigest;
    requires org.elasticsearch.geo;
    requires org.elasticsearch.xcore;
    requires hppc;

    exports org.elasticsearch.compute;
    exports org.elasticsearch.compute.aggregation;
    exports org.elasticsearch.compute.data;
    exports org.elasticsearch.compute.lucene;
    exports org.elasticsearch.compute.operator;
    exports org.elasticsearch.compute.operator.exchange;
    exports org.elasticsearch.compute.aggregation.blockhash;
    exports org.elasticsearch.compute.aggregation.spatial;
    exports org.elasticsearch.compute.operator.lookup;
    exports org.elasticsearch.compute.operator.topn;
    exports org.elasticsearch.compute.operator.mvdedupe;
    exports org.elasticsearch.compute.aggregation.table;
    exports org.elasticsearch.compute.data.sort;
    exports org.elasticsearch.compute.querydsl.query;
}
