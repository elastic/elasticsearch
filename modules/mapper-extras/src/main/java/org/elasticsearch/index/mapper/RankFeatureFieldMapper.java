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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    public static class Builder extends FieldMapper.Builder<Builder> {

        private boolean positiveScoreImpact = true;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder positiveScoreImpact(boolean v) {
            this.positiveScoreImpact = v;
            return builder;
        }

        @Override
        public RankFeatureFieldMapper build(BuilderContext context) {
            return new RankFeatureFieldMapper(name, fieldType, new RankFeatureFieldType(buildFullName(context), meta, positiveScoreImpact),
                multiFieldsBuilder.build(this, context), copyTo, positiveScoreImpact);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            RankFeatureFieldMapper.Builder builder = new RankFeatureFieldMapper.Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("positive_score_impact")) {
                    builder.positiveScoreImpact(XContentMapValues.nodeBooleanValue(propNode));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class RankFeatureFieldType extends MappedFieldType {

        private final boolean positiveScoreImpact;

        public RankFeatureFieldType(String name, Map<String, String> meta, boolean positiveScoreImpact) {
            super(name, true, false, TextSearchInfo.NONE, meta);
            this.positiveScoreImpact = positiveScoreImpact;
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        protected RankFeatureFieldType(RankFeatureFieldType ref) {
            super(ref);
            this.positiveScoreImpact = ref.positiveScoreImpact;
        }

        public RankFeatureFieldType clone() {
            return new RankFeatureFieldType(this);
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
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            throw new IllegalArgumentException("[rank_feature] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new IllegalArgumentException("Queries on [rank_feature] fields are not supported");
        }
    }

    private final boolean positiveScoreImpact;

    private RankFeatureFieldMapper(String simpleName, FieldType fieldType, MappedFieldType mappedFieldType,
                                   MultiFields multiFields, CopyTo copyTo, boolean positiveScoreImpact) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
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
            if (v instanceof Number) {
                value = ((Number) v).floatValue();
            } else {
                value = Float.parseFloat(v.toString());
            }
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

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || positiveScoreImpact == false) {
            builder.field("positive_score_impact", positiveScoreImpact);
        }
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        if (positiveScoreImpact != ((RankFeatureFieldMapper)other).positiveScoreImpact) {
            conflicts.add("mapper [" + name() + "] has different [positive_score_impact] values");
        }
    }
}
