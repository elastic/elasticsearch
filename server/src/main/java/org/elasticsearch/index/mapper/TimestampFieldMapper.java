/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class TimestampFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_timestamp";

    public static class Defaults  {

        public static final FieldType TIMESTAMP_FIELD_TYPE = new FieldType();

        static {
            TIMESTAMP_FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            TIMESTAMP_FIELD_TYPE.freeze();
        }
    }

    // For now the field shouldn't be useable in searches.
    // In the future it should act as an alias to the actual data stream timestamp field.
    public static final class TimestampFieldType extends MappedFieldType {

        public TimestampFieldType() {
            super(NAME, false, false, TextSearchInfo.NONE, Map.of());
        }

        @Override
        public MappedFieldType clone() {
            return new TimestampFieldType();
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support term queries");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support exists queries");
        }

    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder> {

        private String path;

        public Builder() {
            super(NAME, Defaults.TIMESTAMP_FIELD_TYPE);
        }

        public void setPath(String path) {
            this.path = path;
        }

        @Override
        public MetadataFieldMapper build(BuilderContext context) {
            return new TimestampFieldMapper(
                fieldType,
                new TimestampFieldType(),
                path
            );
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {

        @Override
        public MetadataFieldMapper.Builder<?> parse(String name,
                                                    Map<String, Object> node,
                                                    ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder();
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.setPath((String) fieldNode);
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(ParserContext parserContext) {
            return new TimestampFieldMapper(Defaults.TIMESTAMP_FIELD_TYPE,
                new TimestampFieldType(), null);
        }
    }

    private final String path;

    private TimestampFieldMapper(FieldType fieldType, MappedFieldType mappedFieldType, String path) {
        super(fieldType, mappedFieldType);
        this.path = path;
    }

    public void validate(FieldTypeLookup lookup) {
        if (path == null) {
            // not configured, so skip the validation
            return;
        }

        MappedFieldType fieldType = lookup.get(path);
        if (fieldType == null) {
            throw new IllegalArgumentException("timestamp meta field's field_name [" + path + "] points to a non existing field");
        }

        if (DateFieldMapper.CONTENT_TYPE.equals(fieldType.typeName()) == false &&
            DateFieldMapper.DATE_NANOS_CONTENT_TYPE.equals(fieldType.typeName()) == false) {
            throw new IllegalArgumentException("timestamp meta field's field_name [" + path + "] is of type [" +
                fieldType.typeName() + "], but [" + DateFieldMapper.CONTENT_TYPE + "," + DateFieldMapper.DATE_NANOS_CONTENT_TYPE +
                "] is expected");
        }

        if (fieldType.isSearchable() == false) {
            throw new IllegalArgumentException("timestamp meta field's field_name [" + path + "] is not indexed");
        }
        if (fieldType.hasDocValues() == false) {
            throw new IllegalArgumentException("timestamp meta field's field_name [" + path + "] doesn't have doc values");
        }
    }

    @Override
    public void preParse(ParseContext context) throws IOException {

    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        // Meta field doesn't create any fields, so this shouldn't happen.
        throw new IllegalStateException(NAME + " field mapper cannot create fields");
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        if (path == null) {
            // not configured, so skip the validation
            return;
        }

        IndexableField[] fields = context.rootDoc().getFields(path);
        if (fields.length == 0) {
            throw new IllegalArgumentException("data stream timestamp field [" + path + "] is missing");
        }

        long numberOfValues =
            Arrays.stream(fields)
                .filter(indexableField -> indexableField.fieldType().docValuesType() == DocValuesType.SORTED_NUMERIC)
                .count();
        if (numberOfValues > 1) {
            throw new IllegalArgumentException("data stream timestamp field [" + path + "] encountered multiple values");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (path == null) {
            return builder;
        }

        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        builder.startObject(simpleName());
        doXContentBody(builder, includeDefaults, params);
        builder.field("path", path);
        return builder.endObject();
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    @Override
    protected boolean indexedByDefault() {
        return false;
    }

    @Override
    protected boolean docValuesByDefault() {
        return false;
    }
}
