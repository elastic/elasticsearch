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
    requires software.amazon.awssdk.services.bedrockruntime;
    requires software.amazon.awssdk.utils;
    requires software.amazon.awssdk.core;
    requires software.amazon.awssdk.auth;
    requires software.amazon.awssdk.regions;
    requires software.amazon.awssdk.http.nio.netty;
    requires software.amazon.awssdk.profiles;
    requires org.slf4j;
    requires software.amazon.awssdk.retries.api;
    requires org.reactivestreams;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.sslconfig;
    requires org.apache.commons.text;
    requires software.amazon.awssdk.services.sagemakerruntime;

    exports org.elasticsearch.xpack.inference.action;
    exports org.elasticsearch.xpack.inference.registry;
    exports org.elasticsearch.xpack.inference.rest;
    exports org.elasticsearch.xpack.inference.services;
    exports org.elasticsearch.xpack.inference;
    exports org.elasticsearch.xpack.inference.action.task;
    exports org.elasticsearch.xpack.inference.chunking;

    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.xpack.inference.InferenceFeatures;
}
