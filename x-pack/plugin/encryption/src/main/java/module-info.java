/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.encryption {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.encryption.spi;

    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    exports org.elasticsearch.xpack.encryption;

    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.xpack.encryption.EncryptionFeatures;
}
