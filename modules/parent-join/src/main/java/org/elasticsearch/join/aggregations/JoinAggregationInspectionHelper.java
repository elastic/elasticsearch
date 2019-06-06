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
package org.elasticsearch.join.aggregations;

/**
 * Counterpart to {@link org.elasticsearch.search.aggregations.support.AggregationInspectionHelper}, providing
 * helpers for some aggs in the Join package
 */
public class JoinAggregationInspectionHelper {

    public static boolean hasValue(InternalParent agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalChildren agg) {
        return agg.getDocCount() > 0;
    }
}
