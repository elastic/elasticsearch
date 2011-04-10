/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class BinaryFieldMapper extends AbstractFieldMapper<byte[]> {

    public static final String CONTENT_TYPE = "binary";

    public static class Builder extends AbstractFieldMapper.Builder<Builder, BinaryFieldMapper> {

        public Builder(String name) {
            super(name);
            builder = this;
        }

        @Override public Builder indexName(String indexName) {
            return super.indexName(indexName);
        }

        @Override public BinaryFieldMapper build(BuilderContext context) {
            return new BinaryFieldMapper(buildNames(context));
        }
    }

    public static class TypeParser implements XContentMapper.TypeParser {
        @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            BinaryFieldMapper.Builder builder = binaryField(name);
            parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    protected BinaryFieldMapper(Names names) {
        super(names, Field.Index.NO, Field.Store.YES, Field.TermVector.NO, 1.0f, true, true, null, null);
    }

    @Override public byte[] value(Fieldable field) {
        return field.getBinaryValue();
    }

    @Override public byte[] valueFromString(String value) {
        return null;
    }

    @Override public String valueAsString(Fieldable field) {
        return null;
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override protected Field parseCreateField(ParseContext context) throws IOException {
        byte[] value;
        if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        } else {
            value = context.parser().binaryValue();
        }
        if (value == null) {
            return null;
        }
        return new Field(names.indexName(), value);
    }

    @Override protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(names.name());
        builder.field("type", contentType());
        if (!names.name().equals(names.indexNameClean())) {
            builder.field("index_name", names.indexNameClean());
        }
        builder.endObject();
        return builder;
    }
}