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

import org.elasticsearch.index.query.QueryBuilderException;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class FuzzyLikeThisJsonQueryBuilder extends BaseJsonQueryBuilder {

    private final String[] fields;

    private Float boost;

    private String likeText = null;
    private Float minSimilarity;
    private Integer prefixLength;
    private Integer maxNumTerms;
    private Boolean ignoreTF;

    /**
     * Constructs a new fuzzy like this query which uses the "_all" field.
     */
    public FuzzyLikeThisJsonQueryBuilder() {
        this.fields = null;
    }

    /**
     * Sets the field names that will be used when generating the 'Fuzzy Like This' query.
     *
     * @param fields the field names that will be used when generating the 'Fuzzy Like This' query.
     */
    public FuzzyLikeThisJsonQueryBuilder(String... fields) {
        this.fields = fields;
    }

    /**
     * The text to use in order to find documents that are "like" this.
     */
    public FuzzyLikeThisJsonQueryBuilder likeText(String likeText) {
        this.likeText = likeText;
        return this;
    }

    public FuzzyLikeThisJsonQueryBuilder minSimilarity(float minSimilarity) {
        this.minSimilarity = minSimilarity;
        return this;
    }

    public FuzzyLikeThisJsonQueryBuilder prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    public FuzzyLikeThisJsonQueryBuilder maxNumTerms(int maxNumTerms) {
        this.maxNumTerms = maxNumTerms;
        return this;
    }

    public FuzzyLikeThisJsonQueryBuilder ignoreTF(boolean ignoreTF) {
        this.ignoreTF = ignoreTF;
        return this;
    }

    public FuzzyLikeThisJsonQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject(FuzzyLikeThisJsonQueryParser.NAME);
        if (fields != null) {
            builder.startArray("fields");
            for (String field : fields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (likeText == null) {
            throw new QueryBuilderException("fuzzyLikeThis requires 'likeText' to be provided");
        }
        builder.field("like_text", likeText);
        if (maxNumTerms != null) {
            builder.field("max_num_terms", maxNumTerms);
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
        builder.endObject();
    }
}