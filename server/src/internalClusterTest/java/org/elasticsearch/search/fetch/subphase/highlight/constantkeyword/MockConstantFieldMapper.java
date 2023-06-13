/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight.constantkeyword;

import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;

import java.util.Map;

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

        private final Parameter<String> value = Parameter.stringParam("value", false, m -> toType(m).fieldType().value, null);

        protected Builder(String name) {
            super(name);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { value };
        }

        @Override
        public MockConstantFieldMapper build(MapperBuilderContext context) {
            return new MockConstantFieldMapper(name, new MockConstantFieldType(context.buildFullName(name), value.getValue(), Map.of()));
        }
    }

    MockConstantFieldMapper(String indexedValue, MockConstantFieldType fieldType) {
        super(fieldType.name(), fieldType, MultiFields.empty(), CopyTo.empty());
        this.indexedValue = indexedValue;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {}

    @Override
    protected String contentType() {
        return MockConstantFieldType.CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(mappedFieldType.name()).init(this);
    }
}
