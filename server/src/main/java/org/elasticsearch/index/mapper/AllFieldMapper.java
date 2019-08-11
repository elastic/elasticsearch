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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Noop mapper that ensures that mappings created in 6x that explicitly disable the _all field
 * can be restored in this version.
 *
 * TODO: Remove in 8
 */
public class AllFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_all";
    public static final String CONTENT_TYPE = "_all";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new AllFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, AllFieldMapper> {
        private boolean disableExplicit = false;

        public Builder(MappedFieldType existing) {
            super(NAME, existing == null ? Defaults.FIELD_TYPE : existing, Defaults.FIELD_TYPE);
            builder = this;
        }

        private Builder setDisableExplicit() {
            this.disableExplicit = true;
            return this;
        }

        @Override
        public AllFieldMapper build(BuilderContext context) {
            return new AllFieldMapper(fieldType, context.indexSettings(), disableExplicit);
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?,?> parse(String name, Map<String, Object> node,
                                                 ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME));
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                if (fieldName.equals("enabled")) {
                    boolean enabled = XContentMapValues.nodeBooleanValue(entry.getValue(), "enabled");
                    if (enabled) {
                        throw new IllegalArgumentException("[_all] is disabled in this version.");
                    }
                    builder.setDisableExplicit();
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            return new AllFieldMapper(indexSettings, Defaults.FIELD_TYPE, false);
        }
    }

    static final class AllFieldType extends StringFieldType {
        AllFieldType() {
        }

        protected AllFieldType(AllFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new AllFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new MatchNoDocsQuery();
        }
    }

    private final boolean disableExplicit;

    private AllFieldMapper(Settings indexSettings, MappedFieldType existing, boolean disableExplicit) {
        this(existing.clone(), indexSettings, disableExplicit);
    }

    private AllFieldMapper(MappedFieldType fieldType, Settings indexSettings, boolean disableExplicit) {
        super(NAME, fieldType, Defaults.FIELD_TYPE, indexSettings);
        this.disableExplicit = disableExplicit;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // we parse in post parse
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        // noop mapper
        return;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        if (includeDefaults || disableExplicit) {
            builder.startObject(CONTENT_TYPE);
            if (disableExplicit) {
                builder.field("enabled", false);
            }
            builder.endObject();
        }
        return builder;
    }
}
