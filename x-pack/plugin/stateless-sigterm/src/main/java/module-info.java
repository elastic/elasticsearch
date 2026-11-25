/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.elasticsearch.xpack.stateless.shutdown.SigtermHandlerProvider;

module org.elasticsearch.xpack.stateless.sigterm {
    requires org.elasticsearch.server;
    requires org.apache.logging.log4j;
    requires org.elasticsearch.base;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.shutdown;

    exports org.elasticsearch.xpack.stateless.shutdown to org.elasticsearch.server;

    provides org.elasticsearch.node.internal.TerminationHandlerProvider with SigtermHandlerProvider;
}
