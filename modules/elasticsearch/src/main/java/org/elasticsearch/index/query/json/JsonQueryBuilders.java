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
public abstract class JsonQueryBuilders {

    public static MatchAllJsonQueryBuilder matchAllQuery() {
        return new MatchAllJsonQueryBuilder();
    }

    public static DisMaxJsonQueryBuilder disMaxQuery() {
        return new DisMaxJsonQueryBuilder();
    }

    public static TermJsonQueryBuilder termQuery(String name, String value) {
        return new TermJsonQueryBuilder(name, value);
    }

    public static TermJsonQueryBuilder termQuery(String name, int value) {
        return new TermJsonQueryBuilder(name, value);
    }

    public static TermJsonQueryBuilder termQuery(String name, long value) {
        return new TermJsonQueryBuilder(name, value);
    }

    public static TermJsonQueryBuilder termQuery(String name, float value) {
        return new TermJsonQueryBuilder(name, value);
    }

    public static TermJsonQueryBuilder termQuery(String name, double value) {
        return new TermJsonQueryBuilder(name, value);
    }

    public static PrefixJsonQueryBuilder prefixQuery(String name, String value) {
        return new PrefixJsonQueryBuilder(name, value);
    }

    public static RangeJsonQueryBuilder rangeQuery(String name) {
        return new RangeJsonQueryBuilder(name);
    }

    public static WildcardJsonQueryBuilder wildcardQuery(String name, String value) {
        return new WildcardJsonQueryBuilder(name, value);
    }

    public static QueryStringJsonQueryBuilder queryString(String queryString) {
        return new QueryStringJsonQueryBuilder(queryString);
    }

    public static BoolJsonQueryBuilder boolQuery() {
        return new BoolJsonQueryBuilder();
    }

    public static SpanTermJsonQueryBuilder spanTermQuery(String name, String value) {
        return new SpanTermJsonQueryBuilder(name, value);
    }

    public static SpanTermJsonQueryBuilder spanTermQuery(String name, int value) {
        return new SpanTermJsonQueryBuilder(name, value);
    }

    public static SpanTermJsonQueryBuilder spanTermQuery(String name, long value) {
        return new SpanTermJsonQueryBuilder(name, value);
    }

    public static SpanTermJsonQueryBuilder spanTermQuery(String name, float value) {
        return new SpanTermJsonQueryBuilder(name, value);
    }

    public static SpanTermJsonQueryBuilder spanTermQuery(String name, double value) {
        return new SpanTermJsonQueryBuilder(name, value);
    }

    public static SpanFirstJsonQueryBuilder spanFirstQuery(JsonSpanQueryBuilder match, int end) {
        return new SpanFirstJsonQueryBuilder(match, end);
    }

    public static SpanNearJsonQueryBuilder spanNearQuery() {
        return new SpanNearJsonQueryBuilder();
    }

    public static SpanNotJsonQueryBuilder spanNotQuery() {
        return new SpanNotJsonQueryBuilder();
    }

    public static SpanOrJsonQueryBuilder spanOrQuery() {
        return new SpanOrJsonQueryBuilder();
    }

    public static FilteredQueryJsonQueryBuilder filteredQuery(JsonQueryBuilder queryBuilder, JsonFilterBuilder filterBuilder) {
        return new FilteredQueryJsonQueryBuilder(queryBuilder, filterBuilder);
    }

    public static ConstantScoreQueryJsonQueryBuilder constantScoreQuery(JsonFilterBuilder filterBuilder) {
        return new ConstantScoreQueryJsonQueryBuilder(filterBuilder);
    }

    private JsonQueryBuilders() {

    }
}
