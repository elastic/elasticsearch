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

import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Map;


/**
 * A mapper for a builtin field containing metadata about a document.
 */
public abstract class MetadataFieldMapper extends FieldMapper {

    public interface TypeParser extends Mapper.TypeParser {

        @Override
        MetadataFieldMapper.Builder<?,?> parse(String name, Map<String, Object> node,
                                               ParserContext parserContext) throws MapperParsingException;

        /**
         * Get the default {@link MetadataFieldMapper} to use, if nothing had to be parsed.
         * @param fieldType      the existing field type for this meta mapper on the current index
         *                       or null if this is the first type being introduced
         * @param parserContext context that may be useful to build the field like analyzers
         */
        // TODO: remove the fieldType parameter which is only used for bw compat with pre-2.0
        // since settings could be modified
        MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext parserContext);
    }

    @SuppressWarnings("rawtypes")
    public abstract static class Builder<T extends Builder, Y extends MetadataFieldMapper> extends FieldMapper.Builder<T, Y> {
        public Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType) {
            super(name, fieldType, defaultFieldType);
        }
    }

    protected MetadataFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, MultiFields.empty(), CopyTo.empty());
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

    @Override
    public MetadataFieldMapper merge(Mapper mergeWith) {
        return (MetadataFieldMapper) super.merge(mergeWith);
    }
}
