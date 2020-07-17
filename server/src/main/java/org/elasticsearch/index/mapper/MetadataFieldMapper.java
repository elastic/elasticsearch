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

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;


/**
 * A mapper for a builtin field containing metadata about a document.
 */
public abstract class MetadataFieldMapper extends ParametrizedFieldMapper {

    public interface TypeParser extends Mapper.TypeParser {

        @Override
        MetadataFieldMapper.Builder parse(String name, Map<String, Object> node,
                                               ParserContext parserContext) throws MapperParsingException;

        /**
         * Get the default {@link MetadataFieldMapper} to use, if nothing had to be parsed.
         *
         * @param parserContext context that may be useful to build the field like analyzers
         */
        MetadataFieldMapper getDefault(ParserContext parserContext);
    }

    public static class FixedTypeParser implements TypeParser {

        final Function<ParserContext, MetadataFieldMapper> mapperParser;

        public FixedTypeParser(Function<ParserContext, MetadataFieldMapper> mapperParser) {
            this.mapperParser = mapperParser;
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(name + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(ParserContext parserContext) {
            return mapperParser.apply(parserContext);
        }
    }

    public static abstract class Builder extends ParametrizedFieldMapper.Builder {

        protected Builder(String name) {
            super(name);
        }

        boolean isConfigured() {
            for (Parameter<?> param : getParameters()) {
                if (param.isConfigured()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public abstract MetadataFieldMapper build(BuilderContext context);
    }

    protected MetadataFieldMapper(MappedFieldType mappedFieldType) {
        super(mappedFieldType.name(), mappedFieldType, MultiFields.empty(), CopyTo.empty());
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return null;    // by default, things can't be configured so we have no builder
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        MetadataFieldMapper.Builder mergeBuilder = (MetadataFieldMapper.Builder) getMergeBuilder();
        if (mergeBuilder == null || mergeBuilder.isConfigured() == false) {
            return builder;
        }
        return super.toXContent(builder, params);
    }

    /**
     * Called when mapping gets merged. Provides the opportunity to validate other fields a metadata field mapper
     * is supposed to work with before a mapping update is completed.
     */
    public void validate(DocumentFieldMappers lookup) {
        // noop by default
    }

    /**
     * Called before {@link FieldMapper#parse(ParseContext)} on the {@link RootObjectMapper}.
     */
    public abstract void preParse(ParseContext context) throws IOException;

    /**
     * Called after {@link FieldMapper#parse(ParseContext)} on the {@link RootObjectMapper}.
     */
    public void postParse(ParseContext context) throws IOException {
        // do nothing
    }

}
