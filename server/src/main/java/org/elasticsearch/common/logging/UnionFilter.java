/* @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LifeCycle2;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.PerformanceSensitive;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * This is a fork of {@link org.apache.logging.log4j.core.filter.CompositeFilter}. This class
 * also composes and invokes one or more filters, but unlike <code>CompositeFilter</code>, all
 * composed filters will be called to check for one that answers {@link Result#DENY}. The
 * implementation of <code>CompositeFilter</code> makes it impractical to extend.
 */
@Plugin(name = "filters", category = Node.CATEGORY, printObject = true)
@PerformanceSensitive("allocation")
public final class UnionFilter extends AbstractFilter {

    private final Filter[] filters;

    private UnionFilter(final Filter[] filters) {
        this.filters = Objects.requireNonNull(filters, "filters cannot be null");
    }

    @Override
    public void start() {
        this.setStarting();
        for (final Filter filter : filters) {
            filter.start();
        }
        this.setStarted();
    }

    @Override
    public boolean stop(final long timeout, final TimeUnit timeUnit) {
        this.setStopping();
        for (final Filter filter : filters) {
            if (filter instanceof LifeCycle2) {
                ((LifeCycle2) filter).stop(timeout, timeUnit);
            } else {
                filter.stop();
            }
        }
        setStopped();
        return true;
    }

    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg, final Object... params) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, params);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg, final Object p0) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final String msg, final Object p0, final Object p1) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(
        final Logger logger,
        final Level level,
        final Marker marker,
        final String msg,
        final Object p0,
        final Object p1,
        final Object p2
    ) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(
        final Logger logger,
        final Level level,
        final Marker marker,
        final String msg,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3
    ) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(
        final Logger logger,
        final Level level,
        final Marker marker,
        final String msg,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4
    ) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(
        final Logger logger,
        final Level level,
        final Marker marker,
        final String msg,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4,
        final Object p5
    ) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4, p5);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(
        final Logger logger,
        final Level level,
        final Marker marker,
        final String msg,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4,
        final Object p5,
        final Object p6
    ) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4, p5, p6);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(
        final Logger logger,
        final Level level,
        final Marker marker,
        final String msg,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4,
        final Object p5,
        final Object p6,
        final Object p7
    ) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4, p5, p6, p7);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(
        final Logger logger,
        final Level level,
        final Marker marker,
        final String msg,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4,
        final Object p5,
        final Object p6,
        final Object p7,
        final Object p8
    ) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4, p5, p6, p7, p8);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(
        final Logger logger,
        final Level level,
        final Marker marker,
        final String msg,
        final Object p0,
        final Object p1,
        final Object p2,
        final Object p3,
        final Object p4,
        final Object p5,
        final Object p6,
        final Object p7,
        final Object p8,
        final Object p9
    ) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final Object msg, final Throwable t) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, t);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(final Logger logger, final Level level, final Marker marker, final Message msg, final Throwable t) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(logger, level, marker, msg, t);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    @Override
    public Result filter(final LogEvent event) {
        Result result = Result.NEUTRAL;
        for (int i = 0; i < filters.length; i++) {
            result = filters[i].filter(event);
            if (result == Result.DENY) {
                return result;
            }
        }
        return result;
    }

    /**
     * Creates a UnionFilter.
     *
     * @param filters An array of Filters to call.
     * @return The UnionFilter.
     */
    @PluginFactory
    public static UnionFilter createFilters(@PluginElement("Filters") final Filter... filters) {
        return new UnionFilter(filters);
    }

}
