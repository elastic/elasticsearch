/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.query;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 *
 */
public class FuzzyLikeThisFieldQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<FuzzyLikeThisFieldQueryBuilder> {

    private final String name;

    private Float boost;

    private String likeText = null;
    private Float minSimilarity;
    private Integer prefixLength;
    private Integer maxQueryTerms;
    private Boolean ignoreTF;
    private String analyzer;
    private Boolean failOnUnsupportedField;
    private String queryName;

    /**
     * A fuzzy more like this query on the provided field.
     *
     * @param name the name of the field
     */
    public FuzzyLikeThisFieldQueryBuilder(String name) {
        this.name = name;
    }

    /**
     * The text to use in order to find documents that are "like" this.
     */
    public FuzzyLikeThisFieldQueryBuilder likeText(String likeText) {
        this.likeText = likeText;
        return this;
    }

    public FuzzyLikeThisFieldQueryBuilder minSimilarity(float minSimilarity) {
        this.minSimilarity = minSimilarity;
        return this;
    }

    public FuzzyLikeThisFieldQueryBuilder prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    public FuzzyLikeThisFieldQueryBuilder maxQueryTerms(int maxQueryTerms) {
        this.maxQueryTerms = maxQueryTerms;
        return this;
    }

    public FuzzyLikeThisFieldQueryBuilder ignoreTF(boolean ignoreTF) {
        this.ignoreTF = ignoreTF;
        return this;
    }

    /**
     * The analyzer that will be used to analyze the text. Defaults to the analyzer associated with the field.
     */
    public FuzzyLikeThisFieldQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public FuzzyLikeThisFieldQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Whether to fail or return no result when this query is run against a field which is not supported such as binary/numeric fields.
     */
    public FuzzyLikeThisFieldQueryBuilder failOnUnsupportedField(boolean fail) {
        failOnUnsupportedField = fail;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public FuzzyLikeThisFieldQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FuzzyLikeThisFieldQueryParser.NAME);
        builder.startObject(name);
        if (likeText == null) {
            throw new ElasticSearchIllegalArgumentException("fuzzyLikeThis requires 'likeText' to be provided");
        }
        builder.field("like_text", likeText);
        if (maxQueryTerms != null) {
            builder.field("max_query_terms", maxQueryTerms);
        }
        if (minSimilarity != null) {
            builder.field("min_similarity", minSimilarity);
        }
        if (prefixLength != null) {
            builder.field("prefix_length", prefixLength);
        }
        if (ignoreTF != null) {
            builder.field("ignore_tf", ignoreTF);
        }
        if (boost != null) {
            builder.field("boost", boost);
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (failOnUnsupportedField != null) {
            builder.field("fail_on_unsupported_field", failOnUnsupportedField);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
        builder.endObject();
    }
}
