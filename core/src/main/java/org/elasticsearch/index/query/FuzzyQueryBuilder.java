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
public class FuzzyQueryBuilder extends AbstractQueryBuilder<FuzzyQueryBuilder> implements MultiTermQueryBuilder<FuzzyQueryBuilder> {

    public static final String NAME = "fuzzy";

    private final String name;

    private final Object value;

    private Fuzziness fuzziness;

    private Integer prefixLength;

    private Integer maxExpansions;

    //LUCENE 4 UPGRADE  we need a testcase for this + documentation
    private Boolean transpositions;

    private String rewrite;

    static final FuzzyQueryBuilder PROTOTYPE = new FuzzyQueryBuilder(null, null);

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

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(name);
        builder.field("value", value);
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
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
