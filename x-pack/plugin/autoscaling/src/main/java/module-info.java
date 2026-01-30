/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.elasticsearch.reservedstate.ReservedStateHandlerProvider;

module org.elasticsearch.autoscaling {
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;

    exports org.elasticsearch.xpack.autoscaling.action;
    exports org.elasticsearch.xpack.autoscaling.capacity;
    exports org.elasticsearch.xpack.autoscaling;

    provides ReservedStateHandlerProvider with org.elasticsearch.xpack.autoscaling.ReservedAutoscalingStateHandlerProvider;
}
