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

import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class FieldJsonQueryBuilder extends BaseJsonQueryBuilder {

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

    private float fuzzyMinSim = -1;

    private float boost = -1;

    private int fuzzyPrefixLength = -1;

    private int phraseSlop = -1;

    private boolean extraSet = false;

    public FieldJsonQueryBuilder(String name, String query) {
        this(name, (Object) query);
    }

    public FieldJsonQueryBuilder(String name, int query) {
        this(name, (Object) query);
    }

    public FieldJsonQueryBuilder(String name, long query) {
        this(name, (Object) query);
    }

    public FieldJsonQueryBuilder(String name, float query) {
        this(name, (Object) query);
    }

    public FieldJsonQueryBuilder(String name, double query) {
        this(name, (Object) query);
    }

    private FieldJsonQueryBuilder(String name, Object query) {
        this.name = name;
        this.query = query;
    }

    public FieldJsonQueryBuilder boost(float boost) {
        this.boost = boost;
        extraSet = true;
        return this;
    }

    public FieldJsonQueryBuilder defaultOperator(Operator defaultOperator) {
        this.defaultOperator = defaultOperator;
        extraSet = true;
        return this;
    }

    public FieldJsonQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        extraSet = true;
        return this;
    }

    public FieldJsonQueryBuilder allowLeadingWildcard(boolean allowLeadingWildcard) {
        this.allowLeadingWildcard = allowLeadingWildcard;
        extraSet = true;
        return this;
    }

    public FieldJsonQueryBuilder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        extraSet = true;
        return this;
    }

    public FieldJsonQueryBuilder enablePositionIncrements(boolean enablePositionIncrements) {
        this.enablePositionIncrements = enablePositionIncrements;
        extraSet = true;
        return this;
    }

    public FieldJsonQueryBuilder fuzzyMinSim(float fuzzyMinSim) {
        this.fuzzyMinSim = fuzzyMinSim;
        extraSet = true;
        return this;
    }

    public FieldJsonQueryBuilder fuzzyPrefixLength(int fuzzyPrefixLength) {
        this.fuzzyPrefixLength = fuzzyPrefixLength;
        extraSet = true;
        return this;
    }

    public FieldJsonQueryBuilder phraseSlop(int phraseSlop) {
        this.phraseSlop = phraseSlop;
        extraSet = true;
        return this;
    }

    @Override public void doJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject(FieldJsonQueryParser.NAME);
        if (!extraSet) {
            builder.field(name, query);
        } else {
            builder.startObject(name);
            builder.field("query", query);
            if (defaultOperator != null) {
                builder.field("defaultOperator", defaultOperator.name().toLowerCase());
            }
            if (analyzer != null) {
                builder.field("analyzer", analyzer);
            }
            if (allowLeadingWildcard != null) {
                builder.field("allowLeadingWildcard", allowLeadingWildcard);
            }
            if (lowercaseExpandedTerms != null) {
                builder.field("lowercaseExpandedTerms", lowercaseExpandedTerms);
            }
            if (enablePositionIncrements != null) {
                builder.field("enablePositionIncrements", enablePositionIncrements);
            }
            if (fuzzyMinSim != -1) {
                builder.field("fuzzyMinSim", fuzzyMinSim);
            }
            if (boost != -1) {
                builder.field("boost", boost);
            }
            if (fuzzyPrefixLength != -1) {
                builder.field("fuzzyPrefixLength", fuzzyPrefixLength);
            }
            if (phraseSlop != -1) {
                builder.field("phraseSlop", phraseSlop);
            }
            builder.endObject();
        }
        builder.endObject();
    }
}