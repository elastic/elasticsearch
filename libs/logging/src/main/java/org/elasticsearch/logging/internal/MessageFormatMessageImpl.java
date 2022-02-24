/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal;

import org.apache.logging.log4j.message.MessageFormatMessage;
import org.elasticsearch.logging.Message;

public final class MessageFormatMessageImpl implements Message {
    private final MessageFormatMessage mfm;

    public MessageFormatMessageImpl(String messagePattern, final Object... parameters) {
        this.mfm = new MessageFormatMessage(messagePattern, parameters);
    }

    @Override
    public String getFormattedMessage() {
        return mfm.getFormattedMessage();
    }

    @Override
    public String getFormat() {
        return mfm.getFormat();
    }

    @Override
    public Object[] getParameters() {
        return mfm.getParameters();
    }

    @Override
    public Throwable getThrowable() {
        return mfm.getThrowable();
    }
}
