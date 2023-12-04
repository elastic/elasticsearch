/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.repositories.s3 {
    requires org.elasticsearch.server;
    requires aws.java.sdk.core;
    requires org.apache.lucene.core;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.logging;

    exports org.elasticsearch.repositories.s3;
}
