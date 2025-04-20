/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.multiproject {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.apache.lucene.core;
    requires org.elasticsearch.logging;
    requires org.apache.logging.log4j;

    exports org.elasticsearch.multiproject.action to org.elasticsearch.server;

    provides org.elasticsearch.cluster.project.ProjectResolverFactory
        with
            org.elasticsearch.multiproject.TestOnlyMultiProjectResolverFactory;

    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.multiproject.TestOnlyMultiProjectFeatureSpecification;
}
