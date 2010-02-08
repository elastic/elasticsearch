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
 * @author kimchy (Shay Banon)
 */
public class QueryStringJsonQueryBuilder extends BaseJsonQueryBuilder {

    public static enum Operator {
        OR,
        AND
    }

    private final String queryString;

    private String defaultField;

    private Operator defaultOperator;

    private String analyzer;

    private Boolean allowLeadingWildcard;

    private Boolean lowercaseExpandedTerms;

    private Boolean enablePositionIncrements;

    private float fuzzyMinSim = -1;

    private float boost = -1;

    private int fuzzyPrefixLength = -1;

    private int phraseSlop = -1;

    public QueryStringJsonQueryBuilder(String queryString) {
        this.queryString = queryString;
    }

    public QueryStringJsonQueryBuilder defaultField(String defaultField) {
        this.defaultField = defaultField;
        return this;
    }

    public QueryStringJsonQueryBuilder defualtOperator(Operator defaultOperator) {
        this.defaultOperator = defaultOperator;
        return this;
    }

    public QueryStringJsonQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public QueryStringJsonQueryBuilder allowLeadingWildcard(boolean allowLeadingWildcard) {
        this.allowLeadingWildcard = allowLeadingWildcard;
        return this;
    }

    public QueryStringJsonQueryBuilder lowercaseExpandedTerms(boolean lowercaseExpandedTerms) {
        this.lowercaseExpandedTerms = lowercaseExpandedTerms;
        return this;
    }

    public QueryStringJsonQueryBuilder enablePositionIncrements(boolean enablePositionIncrements) {
        this.enablePositionIncrements = enablePositionIncrements;
        return this;
    }

    public QueryStringJsonQueryBuilder fuzzyMinSim(float fuzzyMinSim) {
        this.fuzzyMinSim = fuzzyMinSim;
        return this;
    }

    public QueryStringJsonQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    public QueryStringJsonQueryBuilder fuzzyPrefixLength(int fuzzyPrefixLength) {
        this.fuzzyPrefixLength = fuzzyPrefixLength;
        return this;
    }

    public QueryStringJsonQueryBuilder phraseSlop(int phraseSlop) {
        this.phraseSlop = phraseSlop;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder) throws IOException {
        builder.startObject(QueryStringJsonQueryParser.NAME);
        builder.field("query", queryString);
        if (defaultField != null) {
            builder.field("defaultField", defaultField);
        }
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
}
