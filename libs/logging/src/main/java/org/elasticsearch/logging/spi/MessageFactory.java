/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.spi;

import org.elasticsearch.logging.ESMapMessage;
import org.elasticsearch.logging.Message;

/**
 * An SPI to create messages. Ideally we should get rid of parametrized message and use string suppliers
 * TODO PG ESMapMessage should be more low level and not exposed.
 */
public interface MessageFactory {

    /**
     * Returns the located provider instance.
     */
    static MessageFactory provider() {
        return LoggingSupportProvider.provider().messageFactory();
    }

    Message createParametrizedMessage(String format, Object[] params, Throwable throwable);

    ESMapMessage createMapMessage(String format, Object[] params);

    ESMapMessage createMapMessage();
}
