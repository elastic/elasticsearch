/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
module org.elasticsearch.slm {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;
    requires org.apache.lucene.core;
    requires org.apache.logging.log4j;

    exports org.elasticsearch.xpack.slm.action to org.elasticsearch.server;
    exports org.elasticsearch.xpack.slm to org.elasticsearch.server;

    provides org.elasticsearch.reservedstate.ReservedClusterStateHandlerProvider
        with
            org.elasticsearch.xpack.slm.ReservedLifecycleStateHandlerProvider;

    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.xpack.slm.SnapshotLifecycleFeatures;
}
