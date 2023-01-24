/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.deprecation {
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.base;
    requires org.apache.logging.log4j;
    requires org.apache.logging.log4j.core;
    requires log4j2.ecs.layout;

    exports org.elasticsearch.xpack.deprecation to org.elasticsearch.server;
    exports org.elasticsearch.xpack.deprecation.logging to org.elasticsearch.server;
}
