/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/** Elasticsearch X-Pack Wildcard Plugin. */
module org.elasticsearch.wildcard {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.painless.spi;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;
    requires org.apache.lucene.core;
    requires org.apache.lucene.analysis.common;

    exports org.elasticsearch.xpack.wildcard;

    opens org.elasticsearch.xpack.wildcard to org.elasticsearch.painless.spi;

    provides org.elasticsearch.painless.spi.PainlessExtension with org.elasticsearch.xpack.wildcard.WildcardPainlessExtension;
}
