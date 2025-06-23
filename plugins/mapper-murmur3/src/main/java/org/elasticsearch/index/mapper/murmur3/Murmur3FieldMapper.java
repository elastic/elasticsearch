/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.murmur3;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.murmur3.Murmur3DocValueField;

import java.io.IOException;
import java.util.Map;

public class Murmur3FieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "murmur3";

    private static Murmur3FieldMapper toType(FieldMapper in) {
        return (Murmur3FieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).fieldType().isStored(), false);
        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { stored, meta };
        }

        @Override
        public Murmur3FieldMapper build(MapperBuilderContext context) {
            return new Murmur3FieldMapper(
                leafName(),
                new Murmur3FieldType(context.buildFullName(leafName()), stored.getValue(), meta.getValue()),
                builderParams(this, context)
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    // this only exists so a check can be done to match the field type to using murmur3 hashing...
    public static class Murmur3FieldType extends MappedFieldType {

        private Murmur3FieldType(String name, boolean isStored, Map<String, String> meta) {
            super(name, false, isStored, true, TextSearchInfo.NONE, meta);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), NumericType.LONG, Murmur3DocValueField::new, isIndexed());
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Murmur3 fields are not searchable: [" + name() + "]");
        }
    }

    protected Murmur3FieldMapper(String simpleName, MappedFieldType mappedFieldType, BuilderParams builderParams) {
        super(simpleName, mappedFieldType, builderParams);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName()).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value = context.parser().textOrNull();
        if (value != null) {
            final BytesRef bytes = new BytesRef(value);
            final long hash = MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, new MurmurHash3.Hash128()).h1;
            context.doc().add(new SortedNumericDocValuesField(fieldType().name(), hash));
            if (fieldType().isStored()) {
                context.doc().add(new StoredField(fullPath(), hash));
            }
        }
    }

}
