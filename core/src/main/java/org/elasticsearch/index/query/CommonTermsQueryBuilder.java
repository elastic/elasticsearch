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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * CommonTermsQuery query is a query that executes high-frequency terms in a
 * optional sub-query to prevent slow queries due to "common" terms like
 * stopwords. This query basically builds 2 queries off the {@link #add(Term)
 * added} terms where low-frequency terms are added to a required boolean clause
 * and high-frequency terms are added to an optional boolean clause. The
 * optional clause is only executed if the required "low-frequency' clause
 * matches. Scores produced by this query will be slightly different to plain
 * {@link BooleanQuery} scorer mainly due to differences in the
 * {@link Similarity#coord(int,int) number of leave queries} in the required
 * boolean clause. In the most cases high-frequency terms are unlikely to
 * significantly contribute to the document score unless at least one of the
 * low-frequency terms are matched such that this query can improve query
 * execution times significantly if applicable.
 * <p>
 */
public class CommonTermsQueryBuilder extends QueryBuilder implements BoostableQueryBuilder<CommonTermsQueryBuilder> {

    public static enum Operator {
        OR, AND
    }

    private final String name;

    private final Object text;

    private Operator highFreqOperator = null;

    private Operator lowFreqOperator = null;

    private String analyzer = null;

    private Float boost = null;

    private String lowFreqMinimumShouldMatch = null;

    private String highFreqMinimumShouldMatch = null;

    private Boolean disableCoord = null;

    private Float cutoffFrequency = null;

    private String queryName;

    /**
     * Constructs a new common terms query.
     */
    public CommonTermsQueryBuilder(String name, Object text) {
        if (name == null) {
            throw new IllegalArgumentException("Field name must not be null");
        }
        if (text == null) {
            throw new IllegalArgumentException("Query must not be null");
        }
        this.text = text;
        this.name = name;
    }

    /**
     * Sets the operator to use for terms with a high document frequency
     * (greater than or equal to {@link #cutoffFrequency(float)}. Defaults to
     * <tt>AND</tt>.
     */
    public CommonTermsQueryBuilder highFreqOperator(Operator operator) {
        this.highFreqOperator = operator;
        return this;
    }

    /**
     * Sets the operator to use for terms with a low document frequency (less
     * than {@link #cutoffFrequency(float)}. Defaults to <tt>AND</tt>.
     */
    public CommonTermsQueryBuilder lowFreqOperator(Operator operator) {
        this.lowFreqOperator = operator;
        return this;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping
     * config for the field, or, if not set, the default search analyzer.
     */
    public CommonTermsQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Set the boost to apply to the query.
     */
    @Override
    public CommonTermsQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the cutoff document frequency for high / low frequent terms. A value
     * in [0..1] (or absolute number >=1) representing the maximum threshold of
     * a terms document frequency to be considered a low frequency term.
     * Defaults to
     * <tt>{@value CommonTermsQueryParser#DEFAULT_MAX_TERM_DOC_FREQ}</tt>
     */
    public CommonTermsQueryBuilder cutoffFrequency(float cutoffFrequency) {
        this.cutoffFrequency = cutoffFrequency;
        return this;
    }

    /**
     * Sets the minimum number of high frequent query terms that need to match in order to
     * produce a hit when there are no low frequen terms.
     */
    public CommonTermsQueryBuilder highFreqMinimumShouldMatch(String highFreqMinimumShouldMatch) {
        this.highFreqMinimumShouldMatch = highFreqMinimumShouldMatch;
        return this;
    }

    /**
     * Sets the minimum number of low frequent query terms that need to match in order to
     * produce a hit.
     */
    public CommonTermsQueryBuilder lowFreqMinimumShouldMatch(String lowFreqMinimumShouldMatch) {
        this.lowFreqMinimumShouldMatch = lowFreqMinimumShouldMatch;
        return this;
    }
    
    public CommonTermsQueryBuilder disableCoord(boolean disableCoord) {
        this.disableCoord = disableCoord;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public CommonTermsQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(CommonTermsQueryParser.NAME);
        builder.startObject(name);

        builder.field("query", text);
        if (disableCoord != null) {
            builder.field("disable_coord", disableCoord);
        }
        if (highFreqOperator != null) {
            builder.field("high_freq_operator", highFreqOperator.toString());
        }
        if (lowFreqOperator != null) {
            builder.field("low_freq_operator", lowFreqOperator.toString());
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (boost != null) {
            builder.field("boost", boost);
        }
        if (cutoffFrequency != null) {
            builder.field("cutoff_frequency", cutoffFrequency);
        }
        if (lowFreqMinimumShouldMatch != null || highFreqMinimumShouldMatch != null) {
            builder.startObject("minimum_should_match");
            if (lowFreqMinimumShouldMatch != null) {
                builder.field("low_freq", lowFreqMinimumShouldMatch);
            }
            if (highFreqMinimumShouldMatch != null) {
                builder.field("high_freq", highFreqMinimumShouldMatch);
            }
            builder.endObject();
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }

        builder.endObject();
        builder.endObject();
    }
}
