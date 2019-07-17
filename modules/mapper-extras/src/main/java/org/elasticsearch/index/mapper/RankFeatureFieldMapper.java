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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link FieldMapper} that exposes Lucene's {@link FeatureField}.
 */
public class RankFeatureFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "rank_feature";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new RankFeatureFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, RankFeatureFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public RankFeatureFieldType fieldType() {
            return (RankFeatureFieldType) super.fieldType();
        }

        public Builder positiveScoreImpact(boolean v) {
            fieldType().setPositiveScoreImpact(v);
            return builder;
        }

        @Override
        public RankFeatureFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new RankFeatureFieldMapper(
                    name, fieldType, defaultFieldType,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
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

        private boolean positiveScoreImpact = true;

        public RankFeatureFieldType() {
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
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            RankFeatureFieldType other = (RankFeatureFieldType) o;
            return Objects.equals(positiveScoreImpact, other.positiveScoreImpact);
        }

        @Override
        public int hashCode() {
            int h = super.hashCode();
            h = 31 * h + Objects.hashCode(positiveScoreImpact);
            return h;
        }

        @Override
        public void checkCompatibility(MappedFieldType other, List<String> conflicts) {
            super.checkCompatibility(other, conflicts);
            if (positiveScoreImpact != ((RankFeatureFieldType) other).positiveScoreImpact()) {
                conflicts.add("mapper [" + name() + "] has different [positive_score_impact] values");
            }
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public boolean positiveScoreImpact() {
            return positiveScoreImpact;
        }

        public void setPositiveScoreImpact(boolean positiveScoreImpact) {
            checkIfFrozen();
            this.positiveScoreImpact = positiveScoreImpact;
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

    private RankFeatureFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
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
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
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

        if (fieldType().positiveScoreImpact() == false) {
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

        if (includeDefaults || fieldType().positiveScoreImpact() == false) {
            builder.field("positive_score_impact", fieldType().positiveScoreImpact());
        }
    }
}
