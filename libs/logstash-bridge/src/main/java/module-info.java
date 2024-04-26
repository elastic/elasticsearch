/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/** Elasticsearch Logstash Bridge. */
module org.elasticsearch.logstashbridge {
    requires org.elasticsearch.base;
    requires org.elasticsearch.grok;
    requires org.elasticsearch.server;
    requires org.elasticsearch.painless;
    requires org.elasticsearch.painless.spi;

    exports org.elasticsearch.logstashbridge;
}
