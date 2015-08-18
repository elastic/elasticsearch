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

package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A filer for a field based on several terms matching on any of them.
 */
public class TermsQueryBuilder extends QueryBuilder implements BoostableQueryBuilder<TermsQueryBuilder> {

    private final String name;

    private final Object values;

    private String minimumShouldMatch;

    private Boolean disableCoord;

    private String queryName;

    private float boost = -1;

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, String... values) {
        this(name, (Object[]) values);
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, int... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, long... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, float... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, double... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, Object... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filer for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, Iterable values) {
        this.name = name;
        this.values = values;
    }

    /**
     * Sets the minimum number of matches across the provided terms. Defaults to <tt>1</tt>.
     * @deprecated use [bool] query instead
     */
    @Deprecated
    public TermsQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    /**
     * Disables <tt>Similarity#coord(int,int)</tt> in scoring. Defaults to <tt>false</tt>.
     * @deprecated use [bool] query instead
     */
    @Deprecated
    public TermsQueryBuilder disableCoord(boolean disableCoord) {
        this.disableCoord = disableCoord;
        return this;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public TermsQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    public TermsQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TermsQueryParser.NAME);
        builder.field(name, values);

        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }

        if (disableCoord != null) {
            builder.field("disable_coord", disableCoord);
        }

        if (boost != -1) {
            builder.field("boost", boost);
        }

        if (queryName != null) {
            builder.field("_name", queryName);
        }

        builder.endObject();
    }
}