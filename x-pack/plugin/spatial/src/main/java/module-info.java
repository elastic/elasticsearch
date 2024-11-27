/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.spatial {
    requires org.apache.lucene.spatial3d;
    requires org.elasticsearch.h3;
    requires org.elasticsearch.legacy.geo;
    requires org.elasticsearch.painless.spi;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.base;
    requires org.elasticsearch.geo;
    requires org.apache.lucene.core;

    exports org.elasticsearch.xpack.spatial.action to org.elasticsearch.server;
    exports org.elasticsearch.xpack.spatial.common; // to vector-tile
    exports org.elasticsearch.xpack.spatial; // to vector-tile
    exports org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid; // to vector-tile

    exports org.elasticsearch.xpack.spatial.index.query;
    exports org.elasticsearch.xpack.spatial.index.fielddata;
    exports org.elasticsearch.xpack.spatial.index.fielddata.plain;
    exports org.elasticsearch.xpack.spatial.index.mapper;

    exports org.elasticsearch.xpack.spatial.ingest;
    exports org.elasticsearch.xpack.spatial.script.field;

    exports org.elasticsearch.xpack.spatial.search.aggregations;
    exports org.elasticsearch.xpack.spatial.search.aggregations.metrics;
    exports org.elasticsearch.xpack.spatial.search.aggregations.support;
    exports org.elasticsearch.xpack.spatial.search.runtime;

    opens org.elasticsearch.xpack.spatial to org.elasticsearch.painless.spi; // whitelist resource access

    provides org.elasticsearch.painless.spi.PainlessExtension with org.elasticsearch.xpack.spatial.SpatialPainlessExtension;
}
