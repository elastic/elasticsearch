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

import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A Query that does fuzzy matching for a specific value.
 */
public class FuzzyQueryBuilder extends MultiTermQueryBuilder implements BoostableQueryBuilder<FuzzyQueryBuilder> {

    private final String name;

    private final Object value;

    private float boost = -1;

    private Fuzziness fuzziness;

    private Integer prefixLength;

    private Integer maxExpansions;
    
    //LUCENE 4 UPGRADE  we need a testcase for this + documentation
    private Boolean transpositions;

    private String rewrite;

    private String queryName;

    /**
     * Constructs a new fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String name, String value) {
        this(name, (Object) value);
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String name, int value) {
        this(name, (Object) value);
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String name, long value) {
        this(name, (Object) value);
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String name, float value) {
        this(name, (Object) value);
    }

    /**
     * Constructs a new fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String name, double value) {
        this(name, (Object) value);
    }

    // NO COMMIT: not sure we should also allow boolean?
    /**
     * Constructs a new fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the text
     */
    public FuzzyQueryBuilder(String name, boolean value) {
        this(name, (Object) value);
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public FuzzyQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    public FuzzyQueryBuilder fuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness;
        return this;
    }

    public FuzzyQueryBuilder prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    public FuzzyQueryBuilder maxExpansions(int maxExpansions) {
        this.maxExpansions = maxExpansions;
        return this;
    }
    
    public FuzzyQueryBuilder transpositions(boolean transpositions) {
      this.transpositions = transpositions;
      return this;
    }

    public FuzzyQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public FuzzyQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FuzzyQueryParser.NAME);
        builder.startObject(name);
        builder.field("value", value);
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (transpositions != null) {
            builder.field("transpositions", transpositions);
        }
        if (fuzziness != null) {
            fuzziness.toXContent(builder, params);
        }
        if (prefixLength != null) {
            builder.field("prefix_length", prefixLength);
        }
        if (maxExpansions != null) {
            builder.field("max_expansions", maxExpansions);
        }
        if (rewrite != null) {
            builder.field("rewrite", rewrite);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
        builder.endObject();
    }
}