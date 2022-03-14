/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal;

import org.elasticsearch.logging.Message;

public class MessageImpl implements Message {
    org.apache.logging.log4j.message.Message log4jMessage;

    public MessageImpl(org.apache.logging.log4j.message.Message log4jMessage) {
        this.log4jMessage = log4jMessage;
    }

    @Override
    public String getFormattedMessage() {
        return log4jMessage.getFormattedMessage();
    }

    @Override
    public String getFormat() {
        return log4jMessage.getFormat();
    }

    @Override
    public Object[] getParameters() {
        return log4jMessage.getParameters();
    }

    @Override
    public Throwable getThrowable() {
        return log4jMessage.getThrowable();
    }
}
