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
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A {@link FieldMapper} that exposes Lucene's {@link FeatureField} as a sparse
 * vector of features.
 */
public class RankFeaturesFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "rank_features";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public RankFeaturesFieldMapper build(BuilderContext context) {
            return new RankFeaturesFieldMapper(
                    name, fieldType, new RankFeaturesFieldType(buildFullName(context), meta),
                    multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder(name);
        }
    }

    public static final class RankFeaturesFieldType extends MappedFieldType {

        public RankFeaturesFieldType(String name, Map<String, String> meta) {
            super(name, false, false, TextSearchInfo.NONE, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new IllegalArgumentException("[rank_features] fields do not support [exists] queries");
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            throw new IllegalArgumentException("[rank_features] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new IllegalArgumentException("Queries on [rank_features] fields are not supported");
        }
    }

    private RankFeaturesFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                                    MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
    }

    @Override
    protected RankFeaturesFieldMapper clone() {
        return (RankFeaturesFieldMapper) super.clone();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    @Override
    public RankFeaturesFieldType fieldType() {
        return (RankFeaturesFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("[rank_features] fields can't be used in multi-fields");
        }

        if (context.parser().currentToken() != Token.START_OBJECT) {
            throw new IllegalArgumentException("[rank_features] fields must be json objects, expected a START_OBJECT but got: " +
                    context.parser().currentToken());
        }

        String feature = null;
        for (Token token = context.parser().nextToken(); token != Token.END_OBJECT; token = context.parser().nextToken()) {
            if (token == Token.FIELD_NAME) {
                feature = context.parser().currentName();
            } else if (token == Token.VALUE_NULL) {
                // ignore feature, this is consistent with numeric fields
            } else if (token == Token.VALUE_NUMBER || token == Token.VALUE_STRING) {
                final String key = name() + "." + feature;
                float value = context.parser().floatValue(true);
                if (context.doc().getByKey(key) != null) {
                    throw new IllegalArgumentException("[rank_features] fields do not support indexing multiple values for the same " +
                            "rank feature [" + key + "] in the same document");
                }
                context.doc().addWithKey(key, new FeatureField(name(), feature, value));
            } else {
                throw new IllegalArgumentException("[rank_features] fields take hashes that map a feature to a strictly positive " +
                        "float, but got unexpected token " + token);
            }
        }
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected Object parseSourceValue(Object value, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }
        return value;
    }

    @Override
    protected boolean indexedByDefault() {
        return false;
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
