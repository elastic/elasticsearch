/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.action;

import co.elastic.logging.log4j2.EcsLayout;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AsyncAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginConfiguration;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.elasticsearch.common.logging.ECSJsonLayout;
import org.elasticsearch.common.util.ArrayUtils;

@Plugin(name = "ActionLoggerAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public final class ActionLoggerAppender extends AbstractAppender {
    private final AbstractAppender delegate;

    private ActionLoggerAppender(String name, AbstractAppender delegate) {
        super(name, null, null, true, null);
        this.delegate = delegate;
    }

    @Override
    public void append(LogEvent event) {
        delegate.append(event);
    }

    @PluginFactory
    public static ActionLoggerAppender createAppender(
        @PluginAttribute("name") String name,
        @PluginAttribute("actionType") String actionType,
        @PluginAttribute("basePath") String basePath,
        @PluginConfiguration Configuration config
    ) {
        EcsLayout layout = ECSJsonLayout.newBuilder().setDataset("elasticsearch.actionlog." + actionType).setConfiguration(config).build();
        String fileName = basePath + "_" + actionType + "_log.json";
        String filePattern = basePath + "_" + actionType + "_log-%i.json.gz";
        SizeBasedTriggeringPolicy policy = SizeBasedTriggeringPolicy.createPolicy("1GB");
        DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder().withMax("4").withConfig(config).build();
        var roller = RollingFileAppender.newBuilder()
            .setName(name + "_roller")
            .withFileName(fileName)
            .withFilePattern(filePattern)
            .setLayout(layout)
            .withPolicy(policy)
            .withStrategy(strategy)
            .setConfiguration(config)
            .build();
        config.addAppender(roller);
        var async = AsyncAppender.newBuilder()
            .setName(name + "_async")
            .setConfiguration(config)
            .setBlocking(false)
            .setBufferSize(256)
            .setAppenderRefs(new AppenderRef[] { AppenderRef.createAppenderRef(roller.getName(), null, null) })
            .setShutdownTimeout(1000)
            .build();
        config.addAppender(async);
        return new ActionLoggerAppender(name, async);
    }
}
