/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.joda.time.DateTime;

import java.util.Collection;

/**
 * A {@code date_histogram} aggregation.
 */
public interface DateHistogram extends Histogram {

    static interface Bucket extends Histogram.Bucket {

        /**
         * @return the key as a date construct (in UTC timezone).
         */
        DateTime getKeyAsDate();

    }

    @Override
    Collection<? extends DateHistogram.Bucket> getBuckets();

    @Override
    Bucket getBucketByKey(String key);

    @Override
    Bucket getBucketByKey(Number key);

    Bucket getBucketByKey(DateTime key);

    /**
     * The interval the date histogram is based on.
     */
    static class Interval {

        public static final Interval SECOND = new Interval("1s");
        public static final Interval MINUTE = new Interval("1m");
        public static final Interval HOUR = new Interval("1h");
        public static final Interval DAY = new Interval("1d");
        public static final Interval WEEK = new Interval("1w");
        public static final Interval MONTH = new Interval("1M");
        public static final Interval QUARTER = new Interval("1q");
        public static final Interval YEAR = new Interval("1y");

        public static Interval seconds(int sec) {
            return new Interval(sec + "s");
        }

        public static Interval minutes(int min) {
            return new Interval(min + "m");
        }

        public static Interval hours(int hours) {
            return new Interval(hours + "h");
        }

        public static Interval days(int days) {
            return new Interval(days + "d");
        }

        public static Interval weeks(int weeks) {
            return new Interval(weeks + "w");
        }

        private final String expression;

        public Interval(String expression) {
            this.expression = expression;
        }

        @Override
        public String toString() {
            return expression;
        }
    }
}
