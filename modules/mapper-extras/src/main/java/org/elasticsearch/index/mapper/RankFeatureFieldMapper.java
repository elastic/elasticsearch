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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A {@link FieldMapper} that exposes Lucene's {@link FeatureField}.
 */
public class RankFeatureFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "rank_feature";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    private static RankFeatureFieldType ft(FieldMapper in) {
        return ((RankFeatureFieldMapper)in).fieldType();
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Boolean> positiveScoreImpact
            = Parameter.boolParam("positive_score_impact", false, m -> ft(m).positiveScoreImpact, true);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(positiveScoreImpact, meta);
        }

        @Override
        public RankFeatureFieldMapper build(BuilderContext context) {
            return new RankFeatureFieldMapper(name,
                new RankFeatureFieldType(buildFullName(context), meta.getValue(), positiveScoreImpact.getValue()),
                multiFieldsBuilder.build(this, context), copyTo.build(), positiveScoreImpact.getValue());
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class RankFeatureFieldType extends MappedFieldType {

        private final boolean positiveScoreImpact;

        public RankFeatureFieldType(String name, Map<String, String> meta, boolean positiveScoreImpact) {
            super(name, true, false, false, TextSearchInfo.NONE, meta);
            this.positiveScoreImpact = positiveScoreImpact;
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public boolean positiveScoreImpact() {
            return positiveScoreImpact;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term("_feature", name()));
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            throw new IllegalArgumentException("[rank_feature] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new IllegalArgumentException("Queries on [rank_feature] fields are not supported");
        }
    }

    private final boolean positiveScoreImpact;

    private RankFeatureFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                   MultiFields multiFields, CopyTo copyTo, boolean positiveScoreImpact) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.positiveScoreImpact = positiveScoreImpact;
    }

    @Override
    protected RankFeatureFieldMapper clone() {
        return (RankFeatureFieldMapper) super.clone();
    }

    @Override
    public RankFeatureFieldType fieldType() {
        return (RankFeatureFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        float value;
        if (context.externalValueSet()) {
            Object v = context.externalValue();
            value = objectToFloat(v);
        } else if (context.parser().currentToken() == Token.VALUE_NULL) {
            // skip
            return;
        } else {
            value = context.parser().floatValue();
        }

        if (context.doc().getByKey(name()) != null) {
            throw new IllegalArgumentException("[rank_feature] fields do not support indexing multiple values for the same field [" +
                name() + "] in the same document");
        }

        if (positiveScoreImpact == false) {
            value = 1 / value;
        }

        context.doc().addWithKey(name(), new FeatureField("_feature", name(), value));
    }

    private Float objectToFloat(Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else {
            return Float.parseFloat(value.toString());
        }
    }

    @Override
    public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }
        return new SourceValueFetcher(name(), mapperService, parsesArrayValue()) {
            @Override
            protected Float parseSourceValue(Object value) {
                return objectToFloat(value);
            }
        };
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
