/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.inference {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;

    requires org.apache.httpcomponents.httpclient;
    requires org.apache.logging.log4j;
    requires org.apache.httpcomponents.httpcore;
    requires org.apache.httpcomponents.httpasyncclient;
    requires org.apache.httpcomponents.httpcore.nio;
    requires org.apache.lucene.core;
    requires org.apache.lucene.join;
    requires com.ibm.icu;
    requires com.google.auth.oauth2;
    requires com.google.auth;
    requires com.google.api.client;
    requires com.google.gson;
    requires aws.java.sdk.bedrockruntime;
    requires aws.java.sdk.core;
    requires com.fasterxml.jackson.databind;
    requires org.joda.time;

    exports org.elasticsearch.xpack.inference.action;
    exports org.elasticsearch.xpack.inference.registry;
    exports org.elasticsearch.xpack.inference.rest;
    exports org.elasticsearch.xpack.inference.services;
    exports org.elasticsearch.xpack.inference;

    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.xpack.inference.InferenceFeatures;
}
