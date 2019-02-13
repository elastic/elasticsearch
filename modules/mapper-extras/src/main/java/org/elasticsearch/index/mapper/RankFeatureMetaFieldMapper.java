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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This meta field only exists because rank feature fields index everything into a
 * common _feature field and Elasticsearch has a custom codec that complains
 * when fields exist in the index and not in mappings.
 */
public class RankFeatureMetaFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_feature";

    public static final String CONTENT_TYPE = "_feature";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new RankFeatureMetaFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, RankFeatureMetaFieldMapper> {

        public Builder(MappedFieldType existing) {
            super(NAME, existing == null ? Defaults.FIELD_TYPE : existing, Defaults.FIELD_TYPE);
        }

        @Override
        public RankFeatureMetaFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new RankFeatureMetaFieldMapper(fieldType, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?,?> parse(String name,
                Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            return new Builder(parserContext.mapperService().fullName(NAME));
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            if (fieldType != null) {
                return new RankFeatureMetaFieldMapper(indexSettings, fieldType);
            } else {
                return parse(NAME, Collections.emptyMap(), context)
                        .build(new BuilderContext(indexSettings, new ContentPath(1)));
            }
        }
    }

    public static final class RankFeatureMetaFieldType extends MappedFieldType {

        public RankFeatureMetaFieldType() {
        }

        protected RankFeatureMetaFieldType(RankFeatureMetaFieldType ref) {
            super(ref);
        }

        @Override
        public RankFeatureMetaFieldType clone() {
            return new RankFeatureMetaFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("Cannot run exists query on [_feature]");
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new UnsupportedOperationException("The [_feature] field may not be queried directly");
        }
    }

    private RankFeatureMetaFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing.clone(), indexSettings);
    }

    private RankFeatureMetaFieldMapper(MappedFieldType fieldType, Settings indexSettings) {
        super(NAME, fieldType, Defaults.FIELD_TYPE, indexSettings);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {}

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new AssertionError("Should never be called");
    }

    @Override
    public void postParse(ParseContext context) throws IOException {}

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
