/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link FieldMapper} that exposes Lucene's {@link FeatureField}.
 */
public class RankFeatureFieldMapper extends FieldMapper {

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
        return ((RankFeatureFieldMapper) in).fieldType();
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> positiveScoreImpact = Parameter.boolParam(
            "positive_score_impact",
            false,
            m -> ft(m).positiveScoreImpact,
            true
        );
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Float> nullValue = new Parameter<>(
            "null_value",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : objectToFloat(o),
            m -> ft(m).nullValue,
            XContentBuilder::field,
            Objects::toString
        ).addValidator(value -> {
            if (value != null && value < Float.MIN_NORMAL) {
                throw new IllegalArgumentException(
                    "[null_value] must be a positive normal float for field of type [rank_feature], got "
                        + value
                        + " which is less than the minimum positive normal float: "
                        + Float.MIN_NORMAL
                );
            }
        }).acceptsNull();

        public Builder(String name) {
            super(name);
        }

        Builder nullValue(float nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { positiveScoreImpact, nullValue, meta };
        }

        @Override
        public RankFeatureFieldMapper build(MapperBuilderContext context) {
            return new RankFeatureFieldMapper(
                name,
                new RankFeatureFieldType(
                    context.buildFullName(name),
                    meta.getValue(),
                    positiveScoreImpact.getValue(),
                    nullValue.getValue()
                ),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                positiveScoreImpact.getValue(),
                nullValue.getValue()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class RankFeatureFieldType extends MappedFieldType {

        private final boolean positiveScoreImpact;
        private final Float nullValue;

        public RankFeatureFieldType(String name, Map<String, String> meta, boolean positiveScoreImpact, Float nullValue) {
            super(name, true, false, false, TextSearchInfo.NONE, meta);
            this.positiveScoreImpact = positiveScoreImpact;
            this.nullValue = nullValue;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public boolean positiveScoreImpact() {
            return positiveScoreImpact;
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new TermQuery(new Term("_feature", name()));
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[rank_feature] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return sourceValueFetcher(context.isSourceEnabled() ? context.sourcePath(name()) : Collections.emptySet());
        }

        private SourceValueFetcher sourceValueFetcher(Set<String> sourcePaths) {
            return new SourceValueFetcher(sourcePaths, nullValue) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return objectToFloat(value);
                }
            };
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Queries on [rank_feature] fields are not supported");
        }
    }

    private final boolean positiveScoreImpact;
    private final Float nullValue;

    private RankFeatureFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        boolean positiveScoreImpact,
        Float nullValue
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo, false, null);
        this.positiveScoreImpact = positiveScoreImpact;
        this.nullValue = nullValue;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public RankFeatureFieldType fieldType() {
        return (RankFeatureFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        float value;
        if (context.parser().currentToken() == Token.VALUE_NULL) {
            if (nullValue == null) {
                // skip
                return;
            }
            value = nullValue;
        } else {
            value = context.parser().floatValue();
        }

        if (context.doc().getByKey(name()) != null) {
            throw new IllegalArgumentException(
                "[rank_feature] fields do not support indexing multiple values for the same field [" + name() + "] in the same document"
            );
        }

        if (positiveScoreImpact == false) {
            value = 1 / value;
        }

        context.doc().addWithKey(name(), new FeatureField("_feature", name(), value));
    }

    private static Float objectToFloat(Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else {
            return Float.parseFloat(value.toString());
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
