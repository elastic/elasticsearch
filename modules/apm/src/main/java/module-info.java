/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.telemetry.apm {
    requires java.desktop;
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires io.opentelemetry.context;
    requires io.opentelemetry.api;
    requires io.opentelemetry.sdk.trace;
    requires io.opentelemetry.exporter.otlp;
    requires io.opentelemetry.sdk.common;
    requires io.opentelemetry.sdk.metrics;
    requires io.opentelemetry.sdk;
    requires java.logging;
    requires org.apache.logging.log4j.jul;
    requires java.management;
    requires jdk.management;
    requires jsr305;
    requires org.elasticsearch.telemetry;

    exports org.elasticsearch.telemetry.apm;
}
