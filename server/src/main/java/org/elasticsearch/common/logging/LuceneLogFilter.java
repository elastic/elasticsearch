/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;

import java.util.Map;

/**
 * Filters messages from Lucene which may be confusing to users.
 */
@Plugin(name = "LuceneLogFilter", category = Node.CATEGORY, elementType = Filter.ELEMENT_TYPE)
public class LuceneLogFilter extends AbstractFilter {

    private static final Map<String, String> RULES = Map.of(
        "org.apache.lucene.store.MemorySegmentIndexInputProvider", "Using MemorySegmentIndexInput",
        "org.apache.lucene.util.VectorUtilProvider", "VectorUtilProvider lookup"//,
        //"org.apache.lucene.util.VectorUtilProvider", "WARNING: Java vector incubator module is not readable"
    );

    public LuceneLogFilter() {
        super(Result.DENY, Result.ACCEPT);
    }

    private Result filter(String loggerName, Message msg) {
        return Result.ACCEPT;//RULES.get(loggerName)
    }

    @Override
    public Result filter(LogEvent event) {
        return filter(event.getLoggerName(), event.getMessage());
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
        return filter(logger.getName(), msg);
    }

    @PluginFactory
    public static LuceneLogFilter createFilter() {
        return new LuceneLogFilter();
    }
}
