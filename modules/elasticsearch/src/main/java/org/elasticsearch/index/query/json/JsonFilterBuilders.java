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
 * A static factory for simple "import static" usage.
 *
 * @author kimchy (shay.banon)
 */
public abstract class JsonFilterBuilders {

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermJsonFilterBuilder termFilter(String name, String value) {
        return new TermJsonFilterBuilder(name, value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermJsonFilterBuilder termFilter(String name, int value) {
        return new TermJsonFilterBuilder(name, value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermJsonFilterBuilder termFilter(String name, long value) {
        return new TermJsonFilterBuilder(name, value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermJsonFilterBuilder termFilter(String name, float value) {
        return new TermJsonFilterBuilder(name, value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermJsonFilterBuilder termFilter(String name, double value) {
        return new TermJsonFilterBuilder(name, value);
    }

    /**
     * A filter for a field based on a term.
     *
     * @param name  The field name
     * @param value The term value
     */
    public static TermJsonFilterBuilder termFilter(String name, Object value) {
        return new TermJsonFilterBuilder(name, value);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsJsonFilterBuilder termsFilter(String name, String... values) {
        return new TermsJsonFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsJsonFilterBuilder termsFilter(String name, int... values) {
        return new TermsJsonFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsJsonFilterBuilder termsFilter(String name, long... values) {
        return new TermsJsonFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsJsonFilterBuilder termsFilter(String name, float... values) {
        return new TermsJsonFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsJsonFilterBuilder termsFilter(String name, double... values) {
        return new TermsJsonFilterBuilder(name, values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsJsonFilterBuilder termsFilter(String name, Object... values) {
        return new TermsJsonFilterBuilder(name, values);
    }

    /**
     * A filter that restricts search results to values that have a matching prefix in a given
     * field.
     *
     * @param name   The field name
     * @param prefix The prefix
     */
    public static PrefixJsonFilterBuilder prefixFilter(String name, String prefix) {
        return new PrefixJsonFilterBuilder(name, prefix);
    }

    /**
     * A filter that restricts search results to values that are within the given range.
     *
     * @param name The field name
     */
    public static RangeJsonFilterBuilder rangeFilter(String name) {
        return new RangeJsonFilterBuilder(name);
    }

    /**
     * A filter that simply wraps a query.
     *
     * @param queryBuilder The query to wrap as a filter
     */
    public static QueryJsonFilterBuilder queryFilter(JsonQueryBuilder queryBuilder) {
        return new QueryJsonFilterBuilder(queryBuilder);
    }

    public static BoolJsonFilterBuilder boolFilter() {
        return new BoolJsonFilterBuilder();
    }

    private JsonFilterBuilders() {

    }
}
