/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.logging.impl.provider.AppenderSupportImpl;
import org.elasticsearch.logging.impl.provider.Log4JBootstrapSupportImpl;
import org.elasticsearch.logging.impl.provider.Log4JMessageFactoryImpl;
import org.elasticsearch.logging.impl.provider.Log4jLogManagerFactory;
import org.elasticsearch.logging.impl.provider.LogLevelSupportImpl;
import org.elasticsearch.logging.impl.provider.StringBuildersSupportImpl;
import org.elasticsearch.logging.spi.AppenderSupport;
import org.elasticsearch.logging.spi.LogManagerFactory;
import org.elasticsearch.logging.spi.LoggingBootstrapSupport;
import org.elasticsearch.logging.spi.LogLevelSupport;
import org.elasticsearch.logging.spi.MessageFactory;
import org.elasticsearch.logging.spi.StringBuildersSupport;

module org.elasticsearch.logging.impl {
    requires log4j2.ecs.layout;
    requires ecs.logging.core;
    requires org.apache.logging.log4j;
    requires org.apache.logging.log4j.core;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.core;

    opens org.elasticsearch.logging.impl /*to org.apache.logging.log4j.core*/;

    provides MessageFactory with Log4JMessageFactoryImpl;
    provides LoggingBootstrapSupport with Log4JBootstrapSupportImpl;
    provides LogLevelSupport with LogLevelSupportImpl;
    provides AppenderSupport with AppenderSupportImpl;
    provides StringBuildersSupport with StringBuildersSupportImpl;
    provides LogManagerFactory with Log4jLogManagerFactory;
}
