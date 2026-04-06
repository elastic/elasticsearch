/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.telemetry.apm {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires io.opentelemetry.api;
    requires io.opentelemetry.context;
    requires io.opentelemetry.sdk;
    requires io.opentelemetry.sdk.metrics;
    requires io.opentelemetry.exporter.otlp;
    requires io.opentelemetry.instrumentation.runtime_telemetry;
    requires io.opentelemetry.sdk.common;

    exports org.elasticsearch.telemetry.apm;
    exports org.elasticsearch.telemetry.apm.metrics;
}
