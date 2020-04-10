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

/**
 * A shared interface for aggregations that parse and use "interval" parameters.
 *
 * Provides definitions for the new fixed and calendar intervals, and deprecated
 * defintions for the old interval/dateHisto interval parameters
 */
public interface DateIntervalConsumer<T> {
    @Deprecated
    T interval(long interval);
    @Deprecated
    T dateHistogramInterval(DateHistogramInterval dateHistogramInterval);
    T calendarInterval(DateHistogramInterval interval);
    T fixedInterval(DateHistogramInterval interval);

    @Deprecated
    long interval();
    @Deprecated
    DateHistogramInterval dateHistogramInterval();
}
