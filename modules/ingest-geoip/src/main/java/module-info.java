/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.ingest.geoip {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires com.maxmind.geoip2;
    requires com.maxmind.db;

    exports org.elasticsearch.ingest.geoip.direct to org.elasticsearch.server;
    exports org.elasticsearch.ingest.geoip.stats to org.elasticsearch.server;

    exports org.elasticsearch.ingest.geoip to com.maxmind.db;
}
