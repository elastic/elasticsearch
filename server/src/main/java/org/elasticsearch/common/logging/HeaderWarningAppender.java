/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.Message;

@Plugin(name = "HeaderWarningAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class HeaderWarningAppender extends AbstractAppender {
    public HeaderWarningAppender(String name, Filter filter) {
        super(name, filter, null);
    }

    @Override
    public void append(LogEvent event) {
        final Message message = event.getMessage();

        if (message instanceof ESLogMessage) {
            final ESLogMessage esLogMessage = (ESLogMessage) message;

            String messagePattern = esLogMessage.getMessagePattern();
            Object[] arguments = esLogMessage.getArguments();

            HeaderWarning.addWarning(messagePattern, arguments);
        } else {
            final String formattedMessage = event.getMessage().getFormattedMessage();
            HeaderWarning.addWarning(formattedMessage);
        }
    }

    @PluginFactory
    public static HeaderWarningAppender createAppender(
        @PluginAttribute("name") String name,
        @PluginElement("filter") Filter filter
    ) {
        return new HeaderWarningAppender(name, filter);
    }
}
