/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class UnionFilterTests extends ESTestCase {

    /**
     * Check that if all child filters accepts an event, then the union filter will accept it.
     */
    public void testLogEventFilterAcceptsIfAllChildFiltersAccepts() {
        Filter unionFilter = UnionFilter.createFilters(new ConstantFilter(Filter.Result.ACCEPT), new ConstantFilter(Filter.Result.ACCEPT));
        assertThat(unionFilter.filter(mock(LogEvent.class)), equalTo(Filter.Result.ACCEPT));
    }

    /**
     * Check that if all child filters accepts a message, then the union filter will accept it.
     */
    public void testMessageFilterAcceptsIfAllChildFiltersAccepts() {
        Filter unionFilter = UnionFilter.createFilters(new ConstantFilter(Filter.Result.ACCEPT), new ConstantFilter(Filter.Result.ACCEPT));
        assertThat(
            unionFilter.filter(mock(Logger.class), Level.WARN, mock(Marker.class), mock(Message.class), mock(Throwable.class)),
            equalTo(Filter.Result.ACCEPT)
        );
    }

    /**
     * Check that if one child filter rejects an event, then the union filter will reject it.
     */
    public void testLogEventFilterRejectsIfOneChildFiltersAccepts() {
        Filter unionFilter = UnionFilter.createFilters(new ConstantFilter(Filter.Result.ACCEPT), new ConstantFilter(Filter.Result.DENY));
        assertThat(unionFilter.filter(mock(LogEvent.class)), equalTo(Filter.Result.DENY));
    }

    /**
     * Check that if one child filters rejects a message, then the union filter will reject it.
     */
    public void testMessageFilterRejectsIfOneChildFiltersAccepts() {
        Filter unionFilter = UnionFilter.createFilters(new ConstantFilter(Filter.Result.ACCEPT), new ConstantFilter(Filter.Result.DENY));
        assertThat(
            unionFilter.filter(mock(Logger.class), Level.WARN, mock(Marker.class), mock(Message.class), mock(Throwable.class)),
            equalTo(Filter.Result.DENY)
        );
    }

    /**
     * A simple filter for testing that always yields the same result.
     */
    private static class ConstantFilter extends AbstractFilter {
        private final Result result;

        public ConstantFilter(Result result) {
            this.result = result;
        }

        @Override
        public Result filter(LogEvent event) {
            return this.result;
        }

        @Override
        public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
            return this.result;
        }

        @Override
        public Result filter(Logger logger, Level level, Marker marker, Object msg, Throwable t) {
            return this.result;
        }

        @Override
        public Result filter(Logger logger, Level level, Marker marker, String msg, Object... params) {
            return this.result;
        }

        @Override
        public Result filter(Logger logger, Level level, Marker marker, String msg, Object p0) {
            return this.result;
        }

        @Override
        public Result filter(Logger logger, Level level, Marker marker, String msg, Object p0, Object p1) {
            return this.result;
        }

        @Override
        public Result filter(Logger logger, Level level, Marker marker, String msg, Object p0, Object p1, Object p2) {
            return this.result;
        }

        @Override
        public Result filter(Logger logger, Level level, Marker marker, String msg, Object p0, Object p1, Object p2, Object p3) {
            return this.result;
        }

        @Override
        public Result filter(Logger logger, Level level, Marker marker, String msg, Object p0, Object p1, Object p2, Object p3, Object p4) {
            return this.result;
        }

        @Override
        public Result filter(
            Logger logger,
            Level level,
            Marker marker,
            String msg,
            Object p0,
            Object p1,
            Object p2,
            Object p3,
            Object p4,
            Object p5
        ) {
            return this.result;
        }

        @Override
        public Result filter(
            Logger logger,
            Level level,
            Marker marker,
            String msg,
            Object p0,
            Object p1,
            Object p2,
            Object p3,
            Object p4,
            Object p5,
            Object p6
        ) {
            return this.result;
        }

        @Override
        public Result filter(
            Logger logger,
            Level level,
            Marker marker,
            String msg,
            Object p0,
            Object p1,
            Object p2,
            Object p3,
            Object p4,
            Object p5,
            Object p6,
            Object p7
        ) {
            return this.result;
        }

        @Override
        public Result filter(
            Logger logger,
            Level level,
            Marker marker,
            String msg,
            Object p0,
            Object p1,
            Object p2,
            Object p3,
            Object p4,
            Object p5,
            Object p6,
            Object p7,
            Object p8
        ) {
            return this.result;
        }

        @Override
        public Result filter(
            Logger logger,
            Level level,
            Marker marker,
            String msg,
            Object p0,
            Object p1,
            Object p2,
            Object p3,
            Object p4,
            Object p5,
            Object p6,
            Object p7,
            Object p8,
            Object p9
        ) {
            return this.result;
        }
    }
}
