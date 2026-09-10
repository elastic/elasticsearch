/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.telemetry.apm {
    requires java.management;
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.sslconfig;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.apache.logging.log4j.core;
    requires org.apache.lucene.core;
    requires io.opentelemetry.api;
    requires io.opentelemetry.context;
    requires io.opentelemetry.sdk;
    requires io.opentelemetry.sdk.metrics;
    requires io.opentelemetry.sdk.logs;
    requires io.opentelemetry.sdk.trace;
    requires io.opentelemetry.exporter.otlp;
    requires io.opentelemetry.instrumentation_api;
    requires io.opentelemetry.instrumentation.runtime_telemetry;
    requires io.opentelemetry.sdk.common;
    requires org.elasticsearch.logging;
    requires opentelemetry.disk.buffering;
    requires kotlin.stdlib;

    // opentelemetry-exporter-sender-okhttp has no module-info.class (it uses Automatic-Module-Name only), so it
    // cannot declare its own JPMS dependencies. This pulls okhttp3 into the module graph so the sender can load it.
    // Remove once opentelemetry-exporter-sender-okhttp gains a proper module-info.class.
    requires okhttp3;

    exports org.elasticsearch.telemetry.apm;
}
