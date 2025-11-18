/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.searchbusinessrules {

    requires org.apache.lucene.core;

    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;

    exports org.elasticsearch.xpack.searchbusinessrules;

    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.xpack.searchbusinessrules.SearchBusinessRulesFeatures;
    provides org.elasticsearch.plugins.SearchPlugin with org.elasticsearch.xpack.searchbusinessrules.SearchBusinessRules;
}
