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

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class ExternalMetadataMapper extends MetadataFieldMapper {

    static final String CONTENT_TYPE = "_external_root";
    static final String FIELD_NAME = "_is_external";
    static final String FIELD_VALUE = "true";

    protected ExternalMetadataMapper() {
        super(new FieldType(), new BooleanFieldMapper.BooleanFieldType(FIELD_NAME));
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        // handled in post parse
    }

    @Override
    public Iterator<Mapper> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(CONTENT_TYPE).endObject();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        context.doc().add(new StringField(FIELD_NAME, FIELD_VALUE, Store.YES));
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder> {

        protected Builder() {
            super(FIELD_NAME, new FieldType());
        }

        @Override
        public ExternalMetadataMapper build(BuilderContext context) {
            return new ExternalMetadataMapper();
        }

    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {

        @Override
        public MetadataFieldMapper.Builder<?> parse(String name, Map<String, Object> node,
                                                       ParserContext parserContext) throws MapperParsingException {
            return new Builder();
        }

        @Override
        public MetadataFieldMapper getDefault(ParserContext context) {
            return new ExternalMetadataMapper();
        }

    }

}
