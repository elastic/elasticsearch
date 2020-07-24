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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.checkNull;

/**
 * A field mapper that records fields that have been ignored because they were malformed.
 */
public final class IgnoredFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_ignored";
    public static final String CONTENT_TYPE = "_ignored";

    public static class Defaults {
        public static final String NAME = IgnoredFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType();
        public static final boolean DOC_VALUES = false;

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder> {

        private boolean docValues = IgnoredFieldMapper.Defaults.DOC_VALUES;

        public Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE);
        }

        public IgnoredFieldMapper.Builder docValues(boolean docValues) {
            this.docValues = docValues;
            return builder;
        }

        @Override
        public IgnoredFieldMapper build(BuilderContext context) {
            MappedFieldType fieldType = new IgnoredFieldType(docValues);
            return new IgnoredFieldMapper(fieldType);
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?> parse(String name, Map<String, Object> node,
                                                    ParserContext parserContext) throws MapperParsingException {
            IgnoredFieldMapper.Builder builder = new IgnoredFieldMapper.Builder();
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                checkNull(propName, propNode);
                if (propName.equals("doc_values")) {
                    builder.docValues(XContentMapValues.nodeBooleanValue(propNode, "doc_values"));
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(ParserContext context) {
            MappedFieldType fieldType = new IgnoredFieldType(false);
            return new IgnoredFieldMapper(fieldType);
        }
    }

    public static final class IgnoredFieldType extends StringFieldType {

        private IgnoredFieldType(boolean docValues) {
            super(NAME, true, docValues, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            // This query is not performance sensitive, it only helps assess
            // quality of the data, so we may use a slow query. It shouldn't
            // be too slow in practice since the number of unique terms in this
            // field is bounded by the number of fields in the mappings.
            return new TermRangeQuery(name(), null, null, true, true);
        }

    }

    private final boolean docValues;

    private IgnoredFieldMapper(MappedFieldType fieldType) {
        super(Defaults.FIELD_TYPE, fieldType);
        this.docValues=fieldType.hasDocValues();
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
        // done in post-parse
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        for (String field : context.getIgnoredFields()) {
            context.doc().add(new Field(NAME, field, fieldType));
            if (fieldType().hasDocValues()) {
                final BytesRef binaryValue = new BytesRef(field);
                context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (docValues != Defaults.DOC_VALUES ){
            builder.startObject(CONTENT_TYPE);
            builder.field("doc_values", docValues);
            builder.endObject();
        }
        return builder;
    }

}
