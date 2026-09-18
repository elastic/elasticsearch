/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Extensions to and adapters over Lucene's {@code org.apache.lucene.store} abstractions
 * ({@code Directory}, {@code IndexInput}, {@code IndexOutput}).
 */
module org.elasticsearch.lucene.store {
    requires org.elasticsearch.base;
    requires org.elasticsearch.foreign.adapter;
    requires org.apache.lucene.core;

    exports org.elasticsearch.lucene.store;
}
