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

package org.elasticsearch.index.query.xcontent;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A query that executes the query string against a field. It is a simplified
 * version of {@link QueryStringQueryBuilder} that simply runs against
 * a single field.
 *
 * @author kimchy (shay.banon)
 */
public class FieldQueryBuilder extends BaseQueryBuilder {

    public static enum Operator {
        OR,
        AND
    }

    private final String name;

    private final Object query;

    private Operator defaultOperator;

    private String analyzer;

    private Boolean allowLeadingWildcard;

    private Boolean lowercaseExpandedTerms;

    private Boolean enablePositionIncrements;

    private Boolean analyzeWildcard;

    private float fuzzyMinSim = -1;

    private float boost = -1;

    private int fuzzyPrefixLength = -1;

    private int phraseSlop = -1;

    private boolean extraSet = false;

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public FieldQueryBuilder(String name, String query) {
        this(name, (Object) query);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public FieldQueryBuilder(String name, int query) {
        this(name, (Object) query);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public FieldQueryBuilder(String name, long query) {
        this(name, (Object) query);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public FieldQueryBuilder(String name, float query) {
        this(name, (Object) query);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public FieldQueryBuilder(String name, double query) {
        this(name, (Object) query);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public FieldQueryBuilder(String name, boolean query) {
        this(name, (Object) query);
    }

    /**
     * A query that executes the query string against a field. It is a simplified
     * version of {@link QueryStringQueryBuilder} that simply runs against
     * a single field.
     *
     * @param name  The name of the field
     * @param query The query string
     */
    public FieldQueryBuilder(String name, Object query) {
        this.name = name;
        this.query = query;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    public FieldQueryBuilder boost(float boost) {
        this.boost = boost;
        extraSet = true;
        return this;
    }

    /**
     * Sets the boolean operator of the query parser used to parse the query string.
     *
     * <p>In default mode ({@link FieldQueryBuilder.Operator#OR}) terms without any modifiers
     * are considered optional: for example <code>capital of Hungary</code> is equal to
     * <code>capital OR of OR Hungary</code>.
     *
     * <p>In {@link FieldQueryBuilder.Operator#AND} mode terms are considered to be in conjunction: the
     * above mentioned query is parsed as <code>capital AND of AND Hungary</code>
     */
    public FieldQueryBuilder defaultOperator(Operator defaultOperator) {
        this.defaultOperator = defaultOperator;
        extraSet = true;
        return this;
    }

    /**
     * The optional analyzer used to analyze the query string. Note, if a field has search analyzer
     * defined for it, then it will be used automatically. Defaults to the smart search analyzer.
     */
    public FieldQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        extraSet = true;
        return this;
    }

    /**
     * Should leading wildcards be allowed or not. Defaults to <tt>true</tt>.
     */
    public FieldQueryBuilder allowLeadingWildcard(boolean allowLeadingWildcard) {
        this.allowLeadingWildcard = allowLeadingWildcard;
        extraSet = true;
        return this;
    }

    /**
     * Whether terms of wildcard, prefix, fuzzy and range queries are to be automatically
     * lower-cased or not.  Default is <tt>true</tt>.
     */
    public FieldQueryBuilder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        extraSet = true;
        return this;
    }

    /**
     * Set to <tt>true</tt> to enable position increments in result query. Defaults to
     * <tt>true</tt>.
     *
     * <p>When set, result phrase and multi-phrase queries will be aware of position increments.
     * Useful when e.g. a StopFilter increases the position increment of the token that follows an omitted token.
     */
    public FieldQueryBuilder enablePositionIncrements(boolean enablePositionIncrements) {
        this.enablePositionIncrements = enablePositionIncrements;
        extraSet = true;
        return this;
    }

    /**
     * Set the minimum similarity for fuzzy queries. Default is 0.5f.
     */
    public FieldQueryBuilder fuzzyMinSim(float fuzzyMinSim) {
        this.fuzzyMinSim = fuzzyMinSim;
        extraSet = true;
        return this;
    }

    /**
     * Set the prefix length for fuzzy queries. Default is 0.
     */
    public FieldQueryBuilder fuzzyPrefixLength(int fuzzyPrefixLength) {
        this.fuzzyPrefixLength = fuzzyPrefixLength;
        extraSet = true;
        return this;
    }

    /**
     * Sets the default slop for phrases.  If zero, then exact phrase matches
     * are required. Default value is zero.
     */
    public FieldQueryBuilder phraseSlop(int phraseSlop) {
        this.phraseSlop = phraseSlop;
        extraSet = true;
        return this;
    }

    /**
     * Set to <tt>true</tt> to enable analysis on wildcard and prefix queries.
     */
    public FieldQueryBuilder analyzeWildcard(boolean analyzeWildcard) {
        this.analyzeWildcard = analyzeWildcard;
        extraSet = true;
        return this;
    }

    @Override public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FieldQueryParser.NAME);
        if (!extraSet) {
            builder.field(name, query);
        } else {
            builder.startObject(name);
            builder.field("query", query);
            if (defaultOperator != null) {
                builder.field("default_operator", defaultOperator.name().toLowerCase());
            }
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (allowLeadingWildcard != null) {
                builder.field("allow_leading_wildcard", allowLeadingWildcard);
            }
            if (lowercaseExpandedTerms != null) {
                builder.field("lowercase_expanded_terms", lowercaseExpandedTerms);
            }
            if (enablePositionIncrements != null) {
                builder.field("enable_position_increments", enablePositionIncrements);
            }
            if (fuzzyMinSim != -1) {
                builder.field("fuzzy_min_sim", fuzzyMinSim);
            }
            if (boost != -1) {
                builder.field("boost", boost);
            }
            if (fuzzyPrefixLength != -1) {
                builder.field("fuzzy_prefix_length", fuzzyPrefixLength);
            }
            if (phraseSlop != -1) {
                builder.field("phrase_slop", phraseSlop);
            }
            if (analyzeWildcard != null) {
                builder.field("analyze_wildcard", analyzeWildcard);
            }
            builder.endObject();
        }
        builder.endObject();
    }
}