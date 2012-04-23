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

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Text query is a query that analyzes the text and constructs a query as the result of the analysis. It
 * can construct different queries based on the type provided.
 */
public class TextQueryBuilder extends BaseQueryBuilder {

    public static enum Operator {
        OR,
        AND
    }

    public static enum Type {
        /**
         * The text is analyzed and terms are added to a boolean query.
         */
        BOOLEAN,
        /**
         * The text is analyzed and used as a phrase query.
         */
        PHRASE,
        /**
         * The text is analyzed and used in a phrase query, with the last term acting as a prefix.
         */
        PHRASE_PREFIX
    }

    private final String name;

    private final Object text;

    private Type type;

    private Operator operator;

    private String analyzer;

    private Float boost;

    private Integer slop;

    private String fuzziness;

    private Integer prefixLength;

    private Integer maxExpansions;

    /**
     * Constructs a new text query.
     */
    public TextQueryBuilder(String name, Object text) {
        this.name = name;
        this.text = text;
    }

    /**
     * Sets the type of the text query.
     */
    public TextQueryBuilder type(Type type) {
        this.type = type;
        return this;
    }

    /**
     * Sets the operator to use when using a boolean query. Defaults to <tt>OR</tt>.
     */
    public TextQueryBuilder operator(Operator operator) {
        this.operator = operator;
        return this;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping config for the field, or, if not
     * set, the default search analyzer.
     */
    public TextQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Set the boost to apply to the query.
     */
    public TextQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Set the phrase slop if evaluated to a phrase query type.
     */
    public TextQueryBuilder slop(int slop) {
        this.slop = slop;
        return this;
    }

    /**
     * Sets the minimum similarity used when evaluated to a fuzzy query type. Defaults to "0.5".
     */
    public TextQueryBuilder fuzziness(Object fuzziness) {
        this.fuzziness = fuzziness.toString();
        return this;
    }

    public TextQueryBuilder prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * When using fuzzy or prefix type query, the number of term expansions to use. Defaults to unbounded
     * so its recommended to set it to a reasonable value for faster execution.
     */
    public TextQueryBuilder maxExpansions(int maxExpansions) {
        this.maxExpansions = maxExpansions;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TextQueryParser.NAME);
        builder.startObject(name);

        builder.field("query", text);
        if (type != null) {
            builder.field("type", type.toString().toLowerCase());
        }
        if (operator != null) {
            builder.field("operator", operator.toString());
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (boost != null) {
            builder.field("boost", boost);
        }
        if (slop != null) {
            builder.field("slop", slop);
        }
        if (fuzziness != null) {
            builder.field("fuzziness", fuzziness);
        }
        if (prefixLength != null) {
            builder.field("prefix_length", prefixLength);
        }
        if (maxExpansions != null) {
            builder.field("max_expansions", maxExpansions);
        }

        builder.endObject();
        builder.endObject();
    }
}