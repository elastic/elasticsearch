/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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
        return ((RankFeatureFieldMapper)in).fieldType();
    }

    public static class Builder extends FieldMapper.Builder {

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
        public RankFeatureFieldMapper build(ContentPath contentPath) {
            return new RankFeatureFieldMapper(name,
                new RankFeatureFieldType(buildFullName(contentPath), meta.getValue(), positiveScoreImpact.getValue()),
                multiFieldsBuilder.build(this, contentPath), copyTo.build(), positiveScoreImpact.getValue());
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class RankFeatureFieldType extends MappedFieldType {

        private final boolean positiveScoreImpact;

        public RankFeatureFieldType(String name, Map<String, String> meta, boolean positiveScoreImpact) {
            super(name, true, false, false, TextSearchInfo.NONE, meta);
            this.positiveScoreImpact = positiveScoreImpact;
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
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            throw new IllegalArgumentException("[rank_feature] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("[" + typeName() + "] doesn't support formats.");
            }
            return new SourceValueFetcher(name(), context) {
                @Override
                protected Float parseSourceValue(Object value) {
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

    private RankFeatureFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                   MultiFields multiFields, CopyTo copyTo, boolean positiveScoreImpact) {
        super(simpleName, mappedFieldType, Lucene.KEYWORD_ANALYZER, multiFields, copyTo);
        this.positiveScoreImpact = positiveScoreImpact;
    }

    @Override
    public RankFeatureFieldType fieldType() {
        return (RankFeatureFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        float value;
        if (context.parser().currentToken() == Token.VALUE_NULL) {
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
