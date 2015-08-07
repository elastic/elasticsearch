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
 * A filter for a field based on several terms matching on any of them.
 */
public class TermsQueryBuilder extends AbstractQueryBuilder<TermsQueryBuilder> {

    public static final String NAME = "terms";

    static final TermsQueryBuilder PROTOTYPE = new TermsQueryBuilder(null, (Object) null);

    private final String name;

    private final Object values;

    private String execution;

    private String lookupIndex;
    private String lookupType;
    private String lookupId;
    private String lookupRouting;
    private String lookupPath;
    private Boolean lookupCache;

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, String... values) {
        this(name, (Object[]) values);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, int... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, long... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, float... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, double... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, Object... values) {
        this.name = name;
        this.values = values;
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public TermsQueryBuilder(String name, Iterable values) {
        this.name = name;
        this.values = values;
    }

    /**
     * Sets the execution mode for the terms filter. Cane be either "plain", "bool"
     * "and". Defaults to "plain".
     * @deprecated elasticsearch now makes better decisions on its own
     */
    @Deprecated
    public TermsQueryBuilder execution(String execution) {
        this.execution = execution;
        return this;
    }

    /**
     * Sets the index name to lookup the terms from.
     */
    public TermsQueryBuilder lookupIndex(String lookupIndex) {
        this.lookupIndex = lookupIndex;
        return this;
    }

    /**
     * Sets the index type to lookup the terms from.
     */
    public TermsQueryBuilder lookupType(String lookupType) {
        this.lookupType = lookupType;
        return this;
    }

    /**
     * Sets the doc id to lookup the terms from.
     */
    public TermsQueryBuilder lookupId(String lookupId) {
        this.lookupId = lookupId;
        return this;
    }

    /**
     * Sets the path within the document to lookup the terms from.
     */
    public TermsQueryBuilder lookupPath(String lookupPath) {
        this.lookupPath = lookupPath;
        return this;
    }

    public TermsQueryBuilder lookupRouting(String lookupRouting) {
        this.lookupRouting = lookupRouting;
        return this;
    }

    public TermsQueryBuilder lookupCache(boolean lookupCache) {
        this.lookupCache = lookupCache;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (values == null) {
            builder.startObject(name);
            if (lookupIndex != null) {
                builder.field("index", lookupIndex);
            }
            builder.field("type", lookupType);
            builder.field("id", lookupId);
            if (lookupRouting != null) {
                builder.field("routing", lookupRouting);
            }
            if (lookupCache != null) {
                builder.field("cache", lookupCache);
            }
            builder.field("path", lookupPath);
            builder.endObject();
        } else {
            builder.field(name, values);
        }
        if (execution != null) {
            builder.field("execution", execution);
        }

        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
