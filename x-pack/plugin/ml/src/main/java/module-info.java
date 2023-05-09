/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.ml {
    requires org.elasticsearch.painless.spi;
    requires org.apache.lucene.analysis.common;
    requires org.elasticsearch.geo;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.autoscaling;
    requires org.elasticsearch.cli;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.base;
    requires org.elasticsearch.grok;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.apache.httpcomponents.httpcore;
    requires org.apache.httpcomponents.httpclient;
    requires org.apache.httpcomponents.httpasyncclient;
    requires org.apache.httpcomponents.httpcore.nio;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires org.apache.lucene.join;
    requires commons.math3;
    requires ojalgo;

    opens org.elasticsearch.xpack.ml to org.elasticsearch.painless.spi; // whitelist resource access
    opens org.elasticsearch.xpack.ml.utils; // for exact.properties access

    provides org.elasticsearch.painless.spi.PainlessExtension with org.elasticsearch.xpack.ml.MachineLearningPainlessExtension;
    provides org.elasticsearch.xpack.autoscaling.AutoscalingExtension with org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingExtension;

    exports org.elasticsearch.xpack.ml;
    exports org.elasticsearch.xpack.ml.action;
    exports org.elasticsearch.xpack.ml.autoscaling;
    exports org.elasticsearch.xpack.ml.notifications;
}
