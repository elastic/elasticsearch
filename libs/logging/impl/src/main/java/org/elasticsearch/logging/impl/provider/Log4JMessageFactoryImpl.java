/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl.provider;

import org.elasticsearch.logging.ESMapMessage;
import org.elasticsearch.logging.Message;
import org.elasticsearch.logging.impl.ESLogMessage;
import org.elasticsearch.logging.impl.ParameterizedMessageImpl;
import org.elasticsearch.logging.spi.MessageFactory;

public class Log4JMessageFactoryImpl implements MessageFactory {
    public Log4JMessageFactoryImpl() {}

    @Override
    public Message createParametrizedMessage(String format, Object[] params, Throwable throwable) {
        return new ParameterizedMessageImpl(format, params, throwable);
    }

    @Override
    public ESMapMessage createMapMessage(String format, Object[] params) {
        return new ESLogMessage(format, params);
    }

    @Override
    public ESMapMessage createMapMessage() {
        return new ESLogMessage();
    }
}
