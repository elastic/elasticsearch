/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query.json;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class JsonFilterBuilders {

    public static TermJsonFilterBuilder termFilter(String name, String value) {
        return new TermJsonFilterBuilder(name, value);
    }

    public static TermJsonFilterBuilder termFilter(String name, int value) {
        return new TermJsonFilterBuilder(name, value);
    }

    public static TermJsonFilterBuilder termFilter(String name, long value) {
        return new TermJsonFilterBuilder(name, value);
    }

    public static TermJsonFilterBuilder termFilter(String name, float value) {
        return new TermJsonFilterBuilder(name, value);
    }

    public static TermJsonFilterBuilder termFilter(String name, double value) {
        return new TermJsonFilterBuilder(name, value);
    }

    public static PrefixJsonFilterBuilder prefixFilter(String name, String value) {
        return new PrefixJsonFilterBuilder(name, value);
    }

    public static RangeJsonFilterBuilder rangeFilter(String name) {
        return new RangeJsonFilterBuilder(name);
    }

    public static QueryJsonFilterBuilder queryFilter(JsonQueryBuilder queryBuilder) {
        return new QueryJsonFilterBuilder(queryBuilder);
    }

    private JsonFilterBuilders() {

    }
}
