/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.query.AbstractQueryBuilder.DEFAULT_BOOST;

/**
 * A {@link FieldMapper} that exposes Lucene's {@link FeatureField} as a sparse
 * vector of features.
 */
public class RankFeaturesFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "rank_features";

    private static RankFeaturesFieldType ft(FieldMapper in) {
        return ((RankFeaturesFieldMapper) in).fieldType();
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> positiveScoreImpact = Parameter.boolParam(
            "positive_score_impact",
            false,
            m -> ft(m).positiveScoreImpact,
            true
        );
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { positiveScoreImpact, meta };
        }

        @Override
        public RankFeaturesFieldMapper build(MapperBuilderContext context) {
            return new RankFeaturesFieldMapper(
                name(),
                new RankFeaturesFieldType(context.buildFullName(name()), meta.getValue(), positiveScoreImpact.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo,
                positiveScoreImpact.getValue()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n), notInMultiFields(CONTENT_TYPE));

    public static final class RankFeaturesFieldType extends MappedFieldType {

        private final boolean positiveScoreImpact;

        public RankFeaturesFieldType(String name, Map<String, String> meta, boolean positiveScoreImpact) {
            super(name, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.positiveScoreImpact = positiveScoreImpact;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new IllegalArgumentException("[rank_features] fields do not support [exists] queries");
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[rank_features] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return FeatureField.newLinearQuery(name(), indexedValueForSearch(value), DEFAULT_BOOST);
        }

        private static String indexedValueForSearch(Object value) {
            if (value instanceof BytesRef) {
                return ((BytesRef) value).utf8ToString();
            }
            return value.toString();
        }
    }

    private final boolean positiveScoreImpact;

    private RankFeaturesFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        boolean positiveScoreImpact
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo, false, null);
        this.positiveScoreImpact = positiveScoreImpact;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    public RankFeaturesFieldType fieldType() {
        return (RankFeaturesFieldType) super.fieldType();
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    public void parse(DocumentParserContext context) throws IOException {

        if (context.parser().currentToken() != Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "[rank_features] fields must be json objects, expected a START_OBJECT but got: " + context.parser().currentToken()
            );
        }

        String feature = null;
        try {
            // make sure that we don't expand dots in field names while parsing
            context.path().setWithinLeafObject(true);
            for (Token token = context.parser().nextToken(); token != Token.END_OBJECT; token = context.parser().nextToken()) {
                if (token == Token.FIELD_NAME) {
                    feature = context.parser().currentName();
                    if (feature.contains(".")) {
                        throw new IllegalArgumentException(
                            "[rank_features] fields do not support dots in feature names but found [" + feature + "]"
                        );
                    }
                } else if (token == Token.VALUE_NULL) {
                    // ignore feature, this is consistent with numeric fields
                } else if (token == Token.VALUE_NUMBER || token == Token.VALUE_STRING) {
                    final String key = name() + "." + feature;
                    float value = context.parser().floatValue(true);
                    if (context.doc().getByKey(key) != null) {
                        throw new IllegalArgumentException(
                            "[rank_features] fields do not support indexing multiple values for the same "
                                + "rank feature ["
                                + key
                                + "] in the same document"
                        );
                    }
                    if (positiveScoreImpact == false) {
                        value = 1 / value;
                    }
                    context.doc().addWithKey(key, new FeatureField(name(), feature, value));
                } else {
                    throw new IllegalArgumentException(
                        "[rank_features] fields take hashes that map a feature to a strictly positive "
                            + "float, but got unexpected token "
                            + token
                    );
                }
            }
        } finally {
            context.path().setWithinLeafObject(false);
        }
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
