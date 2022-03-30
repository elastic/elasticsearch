/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.spi.LogLevelSupport;
import org.elasticsearch.logging.spi.MessageFactory;

import java.util.Map;

public class SlowLogger {
    private static final MessageFactory provider = MessageFactory.provider();

    private Logger logger;

    public SlowLogger(String name) {
        this.logger = LogManager.getLogger(name);
        LogLevelSupport.provider().setLevel(this.logger, Level.TRACE);
    }

    public static SlowLogger getLogger(String name) {
        return new SlowLogger(name);
    }

    public void warn(Map<String, Object> fields) {
        logger.warn(provider.createMapMessage().withFields(fields));// TODO PG
    }

    public void info(Map<String, Object> fields) {
        logger.info(provider.createMapMessage().withFields(fields));
    }

    public void debug(Map<String, Object> fields) {
        logger.debug(provider.createMapMessage().withFields(fields));
    }

    public void trace(Map<String, Object> fields) {
        logger.trace(provider.createMapMessage().withFields(fields));
    }
}
