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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class RoutingFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_routing";
    public static final String CONTENT_TYPE = "_routing";

    public static class Defaults {
        public static final String NAME = "_routing";

        public static final MappedFieldType FIELD_TYPE = new RoutingFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setNames(new MappedFieldType.Names(NAME));
            FIELD_TYPE.freeze();
        }

        public static final boolean REQUIRED = false;
        public static final String PATH = null;
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, RoutingFieldMapper> {

        private boolean required = Defaults.REQUIRED;

        private String path = Defaults.PATH;

        public Builder(MappedFieldType existing) {
            super(Defaults.NAME, existing == null ? Defaults.FIELD_TYPE : existing);
        }

        public Builder required(boolean required) {
            this.required = required;
            return builder;
        }

        public Builder path(String path) {
            this.path = path;
            return builder;
        }

        @Override
        public RoutingFieldMapper build(BuilderContext context) {
            return new RoutingFieldMapper(fieldType, required, path, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME));
            if (parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1)) {
                parseField(builder, builder.name, node, parserContext);
            }
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("required")) {
                    builder.required(nodeBooleanValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("path") && parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1)) {
                    builder.path(fieldNode.toString());
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    static final class RoutingFieldType extends MappedFieldType {

        public RoutingFieldType() {
            setFieldDataType(new FieldDataType("string"));
        }

        protected RoutingFieldType(RoutingFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new RoutingFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String value(Object value) {
            if (value == null) {
                return null;
            }
            return value.toString();
        }
    }

    private boolean required;
    private final String path;

    public RoutingFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing == null ? Defaults.FIELD_TYPE.clone() : existing.clone(), Defaults.REQUIRED, Defaults.PATH, indexSettings);
    }

    protected RoutingFieldMapper(MappedFieldType fieldType, boolean required, String path, Settings indexSettings) {
        super(NAME, fieldType, Defaults.FIELD_TYPE, indexSettings);
        this.required = required;
        this.path = path;
    }

    public void markAsRequired() {
        this.required = true;
    }

    public boolean required() {
        return this.required;
    }

    public String path() {
        return this.path;
    }

    public String value(Document document) {
        Field field = (Field) document.getField(fieldType().names().indexName());
        return field == null ? null : (String)fieldType().value(field);
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // no need ot parse here, we either get the routing in the sourceToParse
        // or we don't have routing, if we get it in sourceToParse, we process it in preParse
        // which will always be called
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (context.sourceToParse().routing() != null) {
            String routing = context.sourceToParse().routing();
            if (routing != null) {
                if (fieldType().indexOptions() == IndexOptions.NONE && !fieldType().stored()) {
                    context.ignoredValue(fieldType().names().indexName(), routing);
                    return;
                }
                fields.add(new Field(fieldType().names().indexName(), routing, fieldType()));
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        // if all are defaults, no sense to write it at all
        boolean indexed = fieldType().indexOptions() != IndexOptions.NONE;
        boolean indexedDefault = Defaults.FIELD_TYPE.indexOptions() != IndexOptions.NONE;
        if (!includeDefaults && indexed == indexedDefault &&
                fieldType().stored() == Defaults.FIELD_TYPE.stored() && required == Defaults.REQUIRED && path == Defaults.PATH) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (indexCreatedBefore2x && (includeDefaults || indexed != indexedDefault)) {
            builder.field("index", indexTokenizeOptionToString(indexed, fieldType().tokenized()));
        }
        if (indexCreatedBefore2x && (includeDefaults || fieldType().stored() != Defaults.FIELD_TYPE.stored())) {
            builder.field("store", fieldType().stored());
        }
        if (includeDefaults || required != Defaults.REQUIRED) {
            builder.field("required", required);
        }
        if (indexCreatedBefore2x && (includeDefaults || path != Defaults.PATH)) {
            builder.field("path", path);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }
}
