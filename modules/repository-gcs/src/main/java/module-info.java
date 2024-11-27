/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.repository.gcs {
    requires com.google.api.client;
    requires com.google.api.services.storage;
    requires com.google.auth.oauth2;
    requires gax;
    requires google.api.client;
    requires google.cloud.core;
    requires google.cloud.core.http;
    requires google.cloud.storage;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.base;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    exports org.elasticsearch.repositories.gcs to org.elasticsearch.server;
}
