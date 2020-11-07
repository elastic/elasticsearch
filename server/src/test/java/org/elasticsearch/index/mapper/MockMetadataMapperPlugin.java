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
import org.apache.lucene.document.StringField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Mapper plugin providing a mock metadata field mapper implementation that supports setting its value
 * through the document source.
 */
public class MockMetadataMapperPlugin extends Plugin implements MapperPlugin {

    /**
     * A mock metadata field mapper that supports being set from the document source.
     */
    public static class MockMetadataMapper extends MetadataFieldMapper {

        static final String CONTENT_TYPE = "_mock_metadata";
        static final String FIELD_NAME = "_mock_metadata";

        protected MockMetadataMapper() {
            super(new KeywordFieldMapper.KeywordFieldType(FIELD_NAME));
        }

        @Override
        protected void parseCreateField(ParseContext context) throws IOException {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_STRING) {
                context.doc().add(new StringField(FIELD_NAME, context.parser().text(), Field.Store.YES));
            } else {
                throw new IllegalArgumentException("Field [" + fieldType().name() + "] must be a string.");
            }
        }

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }

        public static final TypeParser PARSER = new FixedTypeParser(c -> new MockMetadataMapper());
    }

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Collections.singletonMap(MockMetadataMapper.CONTENT_TYPE, MockMetadataMapper.PARSER);
    }
}
