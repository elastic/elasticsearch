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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

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

    private final boolean usesBinaryDocValues;

    FlattenedFieldParser(
        String rootFieldFullPath,
        String keyedFieldFullPath,
        String keyedIgnoredValuesFieldFullPath,
        MappedFieldType fieldType,
        int depthLimit,
        int ignoreAbove,
        String nullValue,
        boolean usesBinaryDocValues
    ) {
        this.rootFieldFullPath = rootFieldFullPath;
        this.keyedFieldFullPath = keyedFieldFullPath;
        this.keyedIgnoredValuesFieldFullPath = keyedIgnoredValuesFieldFullPath;
        this.fieldType = fieldType;
        this.depthLimit = depthLimit;
        this.ignoreAbove = ignoreAbove;
        this.nullValue = nullValue;
        this.usesBinaryDocValues = usesBinaryDocValues;
    }

    public void parse(final DocumentParserContext documentParserContext) throws IOException {
        XContentParser parser = documentParserContext.parser();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        ContentPath path = new ContentPath();

        var context = new Context(parser, documentParserContext);
        parseObject(context, path);
    }

    private void parseObject(Context context, ContentPath path) throws IOException {
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
                parseFieldValue(context, token, path, currentName);
            }
        }
    }

    private void parseArray(Context context, ContentPath path, String currentName) throws IOException {
        XContentParser parser = context.parser();
        while (true) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.END_ARRAY) {
                return;
            }
            parseFieldValue(context, token, path, currentName);
        }
    }

    private void parseFieldValue(Context context, XContentParser.Token token, ContentPath path, String currentName) throws IOException {
        XContentParser parser = context.parser();
        if (token == XContentParser.Token.START_OBJECT) {
            path.add(currentName);
            validateDepthLimit(path);
            parseObject(context, path);
            path.remove();
        } else if (token == XContentParser.Token.START_ARRAY) {
            parseArray(context, path, currentName);
        } else if (token.isValue()) {
            String value = parser.text();
            addField(context, path, currentName, value);
        } else if (token == XContentParser.Token.VALUE_NULL) {
            if (nullValue != null) {
                addField(context, path, currentName, nullValue);
            }
        } else {
            // Note that we throw an exception here just to be safe. We don't actually expect to reach
            // this case, since XContentParser verifies that the input is well-formed as it parses.
            throw new IllegalArgumentException("Encountered unexpected token [" + token.toString() + "].");
        }
    }

    private void addField(Context context, ContentPath path, String currentName, String value) {
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
                context.documentParserContext.doc().add(new StoredField(keyedIgnoredValuesFieldFullPath, bytesKeyedValue));
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
        if (fieldType.indexType().hasTerms()) {
            context.documentParserContext.doc().add(new StringField(rootFieldFullPath, bytesValue, Field.Store.NO));
            context.documentParserContext.doc().add(new StringField(keyedFieldFullPath, bytesKeyedValue, Field.Store.NO));
        }

        if (fieldType.hasDocValues()) {
            if (usesBinaryDocValues) {
                MultiValuedBinaryDocValuesField.SeparateCount.addToSeparateCountMultiBinaryFieldInDoc(
                    context.documentParserContext.doc(),
                    rootFieldFullPath,
                    bytesValue
                );
                MultiValuedBinaryDocValuesField.SeparateCount.addToSeparateCountMultiBinaryFieldInDoc(
                    context.documentParserContext.doc(),
                    keyedFieldFullPath,
                    bytesKeyedValue
                );
            } else {
                context.documentParserContext.doc().add(new SortedSetDocValuesField(rootFieldFullPath, bytesValue));
                context.documentParserContext.doc().add(new SortedSetDocValuesField(keyedFieldFullPath, bytesKeyedValue));
            }

            if (fieldType.isDimension() == false || context.documentParserContext().getRoutingFields().isNoop()) {
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
        return new BytesRef(keyedValue.bytes, valueStart, keyedValue.length - (length + 1));
    }

    private record Context(XContentParser parser, DocumentParserContext documentParserContext) {}
}
