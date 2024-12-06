/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A helper class for {@link FlattenedFieldMapper} parses a JSON object
 * and produces a pair of indexable fields for each leaf value.
 */
class FlattenedFieldParser {
    static final String SEPARATOR = "\0";
    private static final byte SEPARATOR_BYTE = '\0';

    private final String rootFieldFullPath;
    private final String keyedFieldFullPath;
    private final String keyedIgnoredValuesFieldFullPath;

    private final MappedFieldType fieldType;
    private final int depthLimit;
    private final int ignoreAbove;
    private final String nullValue;

    FlattenedFieldParser(
        String rootFieldFullPath,
        String keyedFieldFullPath,
        String keyedIgnoredValuesFieldFullPath,
        MappedFieldType fieldType,
        int depthLimit,
        int ignoreAbove,
        String nullValue
    ) {
        this.rootFieldFullPath = rootFieldFullPath;
        this.keyedFieldFullPath = keyedFieldFullPath;
        this.keyedIgnoredValuesFieldFullPath = keyedIgnoredValuesFieldFullPath;
        this.fieldType = fieldType;
        this.depthLimit = depthLimit;
        this.ignoreAbove = ignoreAbove;
        this.nullValue = nullValue;
    }

    public List<IndexableField> parse(final DocumentParserContext documentParserContext) throws IOException {
        XContentParser parser = documentParserContext.parser();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        ContentPath path = new ContentPath();
        List<IndexableField> fields = new ArrayList<>();

        var context = new Context(parser, documentParserContext);
        parseObject(context, path, fields);

        return fields;
    }

    private void parseObject(Context context, ContentPath path, List<IndexableField> fields) throws IOException {
        String currentName = null;
        XContentParser parser = context.parser();
        while (true) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.END_OBJECT) {
                return;
            }

            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else {
                assert currentName != null;
                parseFieldValue(context, token, path, currentName, fields);
            }
        }
    }

    private void parseArray(Context context, ContentPath path, String currentName, List<IndexableField> fields) throws IOException {
        XContentParser parser = context.parser();
        while (true) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.END_ARRAY) {
                return;
            }
            parseFieldValue(context, token, path, currentName, fields);
        }
    }

    private void parseFieldValue(
        Context context,
        XContentParser.Token token,
        ContentPath path,
        String currentName,
        List<IndexableField> fields
    ) throws IOException {
        XContentParser parser = context.parser();
        if (token == XContentParser.Token.START_OBJECT) {
            path.add(currentName);
            validateDepthLimit(path);
            parseObject(context, path, fields);
            path.remove();
        } else if (token == XContentParser.Token.START_ARRAY) {
            parseArray(context, path, currentName, fields);
        } else if (token.isValue()) {
            String value = parser.text();
            addField(context, path, currentName, value, fields);
        } else if (token == XContentParser.Token.VALUE_NULL) {
            if (nullValue != null) {
                addField(context, path, currentName, nullValue, fields);
            }
        } else {
            // Note that we throw an exception here just to be safe. We don't actually expect to reach
            // this case, since XContentParser verifies that the input is well-formed as it parses.
            throw new IllegalArgumentException("Encountered unexpected token [" + token.toString() + "].");
        }
    }

    private void addField(Context context, ContentPath path, String currentName, String value, List<IndexableField> fields) {
        String key = path.pathAsText(currentName);
        if (key.contains(SEPARATOR)) {
            throw new IllegalArgumentException(
                "Keys in [flattened] fields cannot contain the reserved character \\0. Offending key: [" + key + "]."
            );
        }

        String keyedValue = createKeyedValue(key, value);
        BytesRef bytesKeyedValue = new BytesRef(keyedValue);

        if (value.length() > ignoreAbove) {
            if (context.documentParserContext().mappingLookup().isSourceSynthetic()) {
                fields.add(new StoredField(keyedIgnoredValuesFieldFullPath, bytesKeyedValue));
            }
            return;
        }

        // check the keyed value doesn't exceed the IndexWriter.MAX_TERM_LENGTH limit enforced by Lucene at index time
        // in that case we can already throw a more user friendly exception here which includes the offending fields key and value lengths
        if (bytesKeyedValue.length > IndexWriter.MAX_TERM_LENGTH) {
            String msg = "Flattened field ["
                + rootFieldFullPath
                + "] contains one immense field"
                + " whose keyed encoding is longer than the allowed max length of "
                + IndexWriter.MAX_TERM_LENGTH
                + " bytes. Key length: "
                + key.length()
                + ", value length: "
                + value.length()
                + " for key starting with ["
                + key.substring(0, Math.min(key.length(), 50))
                + "]";
            throw new IllegalArgumentException(msg);
        }
        BytesRef bytesValue = new BytesRef(value);
        if (fieldType.isIndexed()) {
            fields.add(new StringField(rootFieldFullPath, bytesValue, Field.Store.NO));
            fields.add(new StringField(keyedFieldFullPath, bytesKeyedValue, Field.Store.NO));
        }

        if (fieldType.hasDocValues()) {
            fields.add(new SortedSetDocValuesField(rootFieldFullPath, bytesValue));
            fields.add(new SortedSetDocValuesField(keyedFieldFullPath, bytesKeyedValue));

            if (fieldType.isDimension() == false) {
                return;
            }

            final String keyedFieldName = FlattenedFieldParser.extractKey(bytesKeyedValue).utf8ToString();
            if (fieldType.isDimension() && fieldType.dimensions().contains(keyedFieldName)) {
                final BytesRef keyedFieldValue = FlattenedFieldParser.extractValue(bytesKeyedValue);
                context.documentParserContext().getRoutingFields().addString(rootFieldFullPath + "." + keyedFieldName, keyedFieldValue);
            }
        }
    }

    private void validateDepthLimit(ContentPath path) {
        if (path.length() + 1 > depthLimit) {
            throw new IllegalArgumentException(
                "The provided [flattened] field [" + rootFieldFullPath + "] exceeds the maximum depth limit of [" + depthLimit + "]."
            );
        }
    }

    static String createKeyedValue(String key, String value) {
        return key + SEPARATOR + value;
    }

    static BytesRef extractKey(BytesRef keyedValue) {
        int length;
        for (length = 0; length < keyedValue.length; length++) {
            if (keyedValue.bytes[keyedValue.offset + length] == SEPARATOR_BYTE) {
                break;
            }
        }
        return new BytesRef(keyedValue.bytes, keyedValue.offset, length);
    }

    static BytesRef extractValue(BytesRef keyedValue) {
        int length;
        for (length = 0; length < keyedValue.length; length++) {
            if (keyedValue.bytes[keyedValue.offset + length] == SEPARATOR_BYTE) {
                break;
            }
        }
        int valueStart = keyedValue.offset + length + 1;
        return new BytesRef(keyedValue.bytes, valueStart, keyedValue.length - valueStart);
    }

    private record Context(XContentParser parser, DocumentParserContext documentParserContext) {}
}
