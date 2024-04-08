/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.datastreams {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    exports org.elasticsearch.datastreams.action to org.elasticsearch.server;
    exports org.elasticsearch.datastreams.lifecycle.action to org.elasticsearch.server;
    exports org.elasticsearch.datastreams.lifecycle;

    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.datastreams.DataStreamFeatures;
}
