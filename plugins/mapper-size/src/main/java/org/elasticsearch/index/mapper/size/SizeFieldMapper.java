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

package org.elasticsearch.index.mapper.size;

import org.elasticsearch.common.Explicit;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SizeFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_size";

    private static SizeFieldMapper toType(FieldMapper in) {
        return (SizeFieldMapper) in;
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Explicit<Boolean>> enabled
            = updateableBoolParam("enabled", m -> toType(m).enabled, false);

        private Builder() {
            super(NAME);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(enabled);
        }

        @Override
        public SizeFieldMapper build(BuilderContext context) {
            return new SizeFieldMapper(enabled.getValue(),
                new NumberFieldMapper.NumberFieldType(NAME, NumberFieldMapper.NumberType.INTEGER));
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder parse(String name, Map<String, Object> node,
                                                       ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder();
            builder.parse(name, parserContext, node);
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(ParserContext context) {
            return new SizeFieldMapper(new Explicit<>(false, false),
                new NumberFieldMapper.NumberFieldType(NAME, NumberFieldMapper.NumberType.INTEGER));
        }
    }

    private final Explicit<Boolean> enabled;

    private SizeFieldMapper(Explicit<Boolean> enabled, MappedFieldType mappedFieldType) {
        super(mappedFieldType);
        this.enabled = enabled;
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    public boolean enabled() {
        return this.enabled.value();
    }

    @Override
    public void preParse(ParseContext context) {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        // we post parse it so we get the size stored, possibly compressed (source will be preParse)
        super.parse(context);
    }

    @Override
    public void parse(ParseContext context) {
        // nothing to do here, we call the parent in postParse
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        if (enabled.value() == false) {
            return;
        }
        final int value = context.sourceToParse().source().length();
        context.doc().addAll(NumberFieldMapper.NumberType.INTEGER.createFields(name(), value, true, true, true));
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }
}
