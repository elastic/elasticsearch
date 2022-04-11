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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.logging.core.Filter;
import org.elasticsearch.logging.core.RateLimitingFilter;
import org.elasticsearch.logging.impl.provider.AppenderSupportImpl;

/**
 * A filter used for throttling deprecation logs.
 * A throttling is based on a combined key which consists of `key` from the logged ESMessage and `x-opaque-id`
 * passed by a user on a HTTP header.
 * This filter works by using a lruKeyCache - a set of keys which prevents a second message with the same key to be logged.
 * The lruKeyCache has a size limited to 128, which when breached will remove the oldest entries.
 * <p>
 * It is possible to disable use of `x-opaque-id` as a key with {@xlink Log4jRateLimitingFilter#setUseXOpaqueId(boolean) }//TODO PG
 *
 * @see <a href="https://logging.apache.org/log4j/2.x/manual/filters.htmlf">Log4j2 Filters</a>
 */
@Plugin(name = "RateLimitingFilter", category = Node.CATEGORY, elementType = org.apache.logging.log4j.core.Filter.ELEMENT_TYPE)
public class Log4jRateLimitingFilter extends AbstractFilter {

    RateLimitingFilter rateLimitingFilter = new RateLimitingFilter();

    public Log4jRateLimitingFilter() {
        this(org.apache.logging.log4j.core.Filter.Result.ACCEPT, org.apache.logging.log4j.core.Filter.Result.DENY);
    }

    public Log4jRateLimitingFilter(
        org.apache.logging.log4j.core.Filter.Result onMatch,
        org.apache.logging.log4j.core.Filter.Result onMismatch
    ) {
        super(onMatch, onMismatch);
    }

    @PluginFactory
    public static Log4jRateLimitingFilter createFilter(
        @PluginAttribute("onMatch") final org.apache.logging.log4j.core.Filter.Result match,
        @PluginAttribute("onMismatch") final org.apache.logging.log4j.core.Filter.Result mismatch
    ) {
        return new Log4jRateLimitingFilter(match, mismatch);
    }

    @Override
    public org.apache.logging.log4j.core.Filter.Result filter(LogEvent event) {
        Filter.Result filter1 = rateLimitingFilter.filter(new LogEventImpl(event));
        return AppenderSupportImpl.mapResult(filter1);
    }

    @Override
    public org.apache.logging.log4j.core.Filter.Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
        Filter.Result filter1 = rateLimitingFilter.filterMessage(new MessageImpl(msg));
        return AppenderSupportImpl.mapResult(filter1);
    }

}
