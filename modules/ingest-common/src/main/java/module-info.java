/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.ingest.common {
    requires org.elasticsearch.base;
    requires org.elasticsearch.dissect;
    requires org.elasticsearch.grok;
    requires org.elasticsearch.painless.spi;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;

    requires org.apache.httpcomponents.httpclient;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.analysis.common;
    requires org.jruby.joni;

    exports org.elasticsearch.ingest.common; // for painless

    opens org.elasticsearch.ingest.common to org.elasticsearch.painless.spi; // whitelist resource access

    provides org.elasticsearch.painless.spi.PainlessExtension with org.elasticsearch.ingest.common.ProcessorsWhitelistExtension;
}
