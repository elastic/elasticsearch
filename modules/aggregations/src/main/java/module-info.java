/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.aggs {
    requires org.elasticsearch.base;
    requires org.elasticsearch.painless.spi;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.apache.lucene.core;

    exports org.elasticsearch.aggregations.bucket.histogram;
    exports org.elasticsearch.aggregations.bucket.adjacency;
    exports org.elasticsearch.aggregations.bucket.timeseries;
    exports org.elasticsearch.aggregations.pipeline;
    exports org.elasticsearch.aggregations.metric;

    opens org.elasticsearch.aggregations to org.elasticsearch.painless.spi; // whitelist resource access

    provides org.elasticsearch.painless.spi.PainlessExtension with org.elasticsearch.aggregations.AggregationsPainlessExtension;

    provides org.elasticsearch.plugins.spi.NamedXContentProvider
        with
            org.elasticsearch.aggregations.metric.MatrixStatsNamedXContentProvider;
}
