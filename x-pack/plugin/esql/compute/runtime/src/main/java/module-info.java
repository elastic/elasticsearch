/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.compute.runtime {
    requires org.elasticsearch.compute;
    requires org.elasticsearch.compute.ann;
    requires org.elasticsearch.server;
    requires org.elasticsearch.base;
    requires net.bytebuddy;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    exports org.elasticsearch.compute.runtime;
}
