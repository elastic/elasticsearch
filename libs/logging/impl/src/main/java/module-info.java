/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.logging.impl.provider.LoggingSupportProviderImpl;
import org.elasticsearch.logging.spi.LoggingSupportProvider;

module org.elasticsearch.logging.impl {
    requires log4j2.ecs.layout;
    requires ecs.logging.core;
    requires org.apache.logging.log4j;
    requires org.apache.logging.log4j.core;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.base;

    opens org.elasticsearch.logging.impl /*to org.apache.logging.log4j.core*/;

    provides LoggingSupportProvider with LoggingSupportProviderImpl;
}
