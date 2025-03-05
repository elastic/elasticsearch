/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/** Elasticsearch Logstash Bridge. */
module org.elasticsearch.logstashbridge {
    requires org.elasticsearch.base;
    requires org.elasticsearch.grok;
    requires org.elasticsearch.server;
    requires org.elasticsearch.painless;
    requires org.elasticsearch.painless.spi;
    requires org.elasticsearch.mustache;
    requires org.elasticsearch.xcontent;

    exports org.elasticsearch.logstashbridge;
    exports org.elasticsearch.logstashbridge.common;
    exports org.elasticsearch.logstashbridge.core;
    exports org.elasticsearch.logstashbridge.env;
    exports org.elasticsearch.logstashbridge.ingest;
    exports org.elasticsearch.logstashbridge.plugins;
    exports org.elasticsearch.logstashbridge.script;
    exports org.elasticsearch.logstashbridge.threadpool;
}
