/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight.constantkeyword;

import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Map;
import java.util.Objects;

class MockConstantFieldMapper extends FieldMapper {
    @Override
    public MockConstantFieldType fieldType() {
        return (MockConstantFieldType) super.fieldType();
    }

    final String indexedValue;
    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    private static MockConstantFieldMapper toType(FieldMapper in) {
        return (MockConstantFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        // This is defined as updateable because it can be updated once, from [null] to any value,
        // by a dynamic mapping update. Once it has been set, however, the value cannot be changed.
        private final Parameter<String> value = new Parameter<>("value", true, () -> null, (n, c, o) -> {
            if (o instanceof Number == false && o instanceof CharSequence == false) {
                throw new MapperParsingException(
                    "Property [value] on field [" + n + "] must be a number or a string, but got [" + o + "]"
                );
            }
            return o.toString();
        }, m -> toType(m).fieldType().value, XContentBuilder::field, Objects::toString);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        protected Builder(String name) {
            super(name);
            value.setSerializerCheck((id, ic, v) -> v != null);
            value.setMergeValidator((previous, current, c) -> previous == null || Objects.equals(previous, current));
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[]{value, meta};
        }

        @Override
        public MockConstantFieldMapper build(MapperBuilderContext context) {
            return new MockConstantFieldMapper(
                name,
                new MockConstantFieldType(context.buildFullName(name), value.getValue(), meta.getValue())
            );
        }
    }

    MockConstantFieldMapper(String indexedValue, MockConstantFieldType fieldType) {
        super(fieldType.name(), fieldType, MultiFields.empty(), CopyTo.empty());
        this.indexedValue = indexedValue;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
    }

    @Override
    protected String contentType() {
        return MockConstantFieldType.CONTENT_TYPE;
    }

    @Override
    public Builder getMergeBuilder() {
        return new Builder(mappedFieldType.name());
    }
}
