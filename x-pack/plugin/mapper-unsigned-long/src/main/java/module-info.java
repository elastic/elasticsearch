/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.mapper.unsignedlong {
    requires org.apache.lucene.core;
    requires org.elasticsearch.server;
    requires org.apache.lucene.sandbox;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.painless.spi;

    exports org.elasticsearch.xpack.unsignedlong; // for the painless script engine

    opens org.elasticsearch.xpack.unsignedlong to org.elasticsearch.painless.spi; // whitelist resource access

    provides org.elasticsearch.painless.spi.PainlessExtension with org.elasticsearch.xpack.unsignedlong.DocValuesWhitelistExtension;
}
