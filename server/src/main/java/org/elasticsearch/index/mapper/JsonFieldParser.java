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
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A helper class for {@link JsonFieldMapper} parses a JSON object
 * and produces a pair of indexable fields for each leaf value.
 */
public class JsonFieldParser {
    private static final String SEPARATOR = "\0";

    private final MappedFieldType fieldType;
    private final int ignoreAbove;

    private final String fieldName;
    private final String prefixedFieldName;

    JsonFieldParser(MappedFieldType fieldType,
                    int ignoreAbove) {
        this.fieldType = fieldType;
        this.ignoreAbove = ignoreAbove;

        this.fieldName = fieldType.name();
        this.prefixedFieldName = fieldType.name() + JsonFieldMapper.PREFIXED_FIELD_SUFFIX;
    }

    public List<IndexableField> parse(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT,
            parser.currentToken(),
            parser::getTokenLocation);

        ContentPath path = new ContentPath();
        List<IndexableField> fields = new ArrayList<>();

        parseObject(parser, path, fields);
        return fields;
    }

    private void parseObject(XContentParser parser,
                             ContentPath path,
                             List<IndexableField> fields) throws IOException {
        String currentName = null;
        while (true) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.END_OBJECT) {
                return;
            }

            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                assert currentName != null;
                path.add(currentName);
                parseObject(parser, path, fields);
                path.remove();
            } else if (token == XContentParser.Token.START_ARRAY) {
                assert currentName != null;
                parseArray(parser, path, currentName, fields);
            }  else if (token.isValue()) {
                String value = parser.text();
                addField(path, currentName, value, fields);
            } else if (token == XContentParser.Token.VALUE_NULL) {
                String value = fieldType.nullValueAsString();
                if (value != null) {
                    addField(path, currentName, value, fields);
                }
            }
        }
    }

    private void parseArray(XContentParser parser,
                             ContentPath path,
                             String currentName,
                             List<IndexableField> fields) throws IOException {
        while (true) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.END_ARRAY) {
                return;
            }

            if (token == XContentParser.Token.START_OBJECT) {
                path.add(currentName);
                parseObject(parser, path, fields);
                path.remove();
            } else if (token.isValue()) {
                String value = parser.text();
                addField(path, currentName, value, fields);
            } else if (token == XContentParser.Token.VALUE_NULL) {
                String value = fieldType.nullValueAsString();
                if (value != null) {
                    addField(path, currentName, value, fields);
                }
            }
        }
    }

    private void addField(ContentPath path,
                          String currentName,
                          String value,
                          List<IndexableField> fields) {
        if (value.length() > ignoreAbove) {
            return;
        }

        assert currentName != null;
        String key = path.pathAsText(currentName);
        if (key.contains(SEPARATOR)) {
            throw new IllegalArgumentException("Keys in [json] fields cannot contain the reserved character \\0."
                + " Offending key: [" + key + "].");
        }
        String prefixedValue = createPrefixedValue(key, value);
        
        fields.add(new Field(fieldName, new BytesRef(value), fieldType));
        fields.add(new Field(prefixedFieldName, new BytesRef(prefixedValue), fieldType));
    }

    private static String createPrefixedValue(String key, String value) {
        return key + SEPARATOR + value;
    }
}
