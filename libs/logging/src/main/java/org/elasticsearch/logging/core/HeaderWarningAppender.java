/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.core;

import org.elasticsearch.logging.ESMapMessage;
import org.elasticsearch.logging.Message;
import org.elasticsearch.logging.spi.ServerSupport;

public class HeaderWarningAppender implements Appender {

    public HeaderWarningAppender() {}

    public static HeaderWarningAppender createAppender(String name, Filter filter) {
        return new HeaderWarningAppender();
    }

    @Override
    public void append(LogEvent event) {
        final Message message = event.getMessage();

        if (message instanceof final ESMapMessage esLogMessage) {

            String messagePattern = esLogMessage.getMessagePattern();
            Object[] arguments = esLogMessage.getArguments();

            ServerSupport.INSTANCE.addHeaderWarning(messagePattern, arguments);
        } else {
            final String formattedMessage = event.getMessage().getFormattedMessage();
            ServerSupport.INSTANCE.addHeaderWarning(formattedMessage);
        }
    }

    @Override
    public Filter filter() {
        return null;
    }

    @Override
    public Layout layout() {
        return null;
    }

    @Override
    public String name() {
        return null;
    }
}
