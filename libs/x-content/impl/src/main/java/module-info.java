/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.xcontent.impl {
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.dataformat.cbor;
    requires com.fasterxml.jackson.dataformat.smile;
    requires com.fasterxml.jackson.dataformat.yaml;
    requires org.elasticsearch.base;
    requires org.elasticsearch.xcontent;

    provides org.elasticsearch.xcontent.spi.XContentProvider with org.elasticsearch.xcontent.provider.XContentProviderImpl;
}
