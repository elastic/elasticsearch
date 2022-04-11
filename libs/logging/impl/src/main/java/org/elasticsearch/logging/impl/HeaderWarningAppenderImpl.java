/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl;/*
                                       * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
                                       * or more contributor license agreements. Licensed under the Elastic License
                                       * 2.0 and the Server Side Public License, v 1; you may not use this file except
                                       * in compliance with, at your election, the Elastic License 2.0 or the Server
                                       * Side Public License, v 1.
                                       */

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.elasticsearch.logging.core.HeaderWarningAppender;

@Plugin(name = "HeaderWarningAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class HeaderWarningAppenderImpl extends AbstractAppender {
    HeaderWarningAppender headerWarningAppender = new HeaderWarningAppender();

    public HeaderWarningAppenderImpl(String name, Filter filter) {
        super(name, filter, null);
    }

    @Override
    public void append(LogEvent event) {
        headerWarningAppender.append(new LogEventImpl(event));
    }

    @PluginFactory
    public static HeaderWarningAppenderImpl createAppender(@PluginAttribute("name") String name, @PluginElement("filter") Filter filter) {
        return new HeaderWarningAppenderImpl(name, filter);
    }
}
