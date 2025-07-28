/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.transform {
    requires org.apache.lucene.analysis.common;
    requires org.elasticsearch.geo;
    requires org.elasticsearch.logging;
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
    requires org.elasticsearch.plugin;

    exports org.elasticsearch.xpack.transform;
    exports org.elasticsearch.xpack.transform.action;
    exports org.elasticsearch.xpack.transform.checkpoint;
    exports org.elasticsearch.xpack.transform.notifications;
    exports org.elasticsearch.xpack.transform.persistence;
    exports org.elasticsearch.xpack.transform.rest.action;
    exports org.elasticsearch.xpack.transform.transforms;
    exports org.elasticsearch.xpack.transform.transforms.common;
    exports org.elasticsearch.xpack.transform.transforms.latest;
    exports org.elasticsearch.xpack.transform.transforms.pivot;
    exports org.elasticsearch.xpack.transform.transforms.scheduling;
    exports org.elasticsearch.xpack.transform.utils;
}
