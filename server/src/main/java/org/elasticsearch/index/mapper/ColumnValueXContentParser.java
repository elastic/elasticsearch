/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.ColumnType;
import org.elasticsearch.action.bulk.FieldColumn;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.List;

/**
 * An XContentParser adapter that presents column values from a {@link FieldColumn} as an XContentParser.
 * This allows existing {@link FieldMapper#parse(DocumentParserContext)} code to work unchanged with
 * columnar batch data.
 *
 * <p>Three modes of operation:</p>
 * <ul>
 *   <li><b>Leaf scalar</b>: Presents a single typed value (int, long, string, boolean) as a one-token parser.</li>
 *   <li><b>Binary delegate</b>: Wraps raw XContent bytes (for arrays/nested) in a standard XContentParser.</li>
 *   <li><b>Composite object</b>: Synthesizes an object token stream from grouped child columns.</li>
 * </ul>
 */
public class ColumnValueXContentParser extends AbstractXContentParser {

    /**
     * Create a parser for a leaf scalar value from a column.
     */
    public static ColumnValueXContentParser forLeafValue(FieldColumn column, int docIndex) {
        return new ColumnValueXContentParser(column, docIndex, null, null);
    }

    /**
     * Create a parser that delegates to a standard XContentParser wrapping raw binary (array/nested) data.
     */
    public static XContentParser forBinary(FieldColumn column, int docIndex, XContentType xContentType) throws IOException {
        BytesReference bytes = column.binaryValue(docIndex);
        if (bytes == null) {
            return forNullValue();
        }
        return xContentType.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, bytes.streamInput());
    }

    /**
     * Create a parser that synthesizes an object token stream from grouped child columns.
     * Used for composite mappers (geo_point, range, etc.) where the object was flattened to dot-path columns.
     *
     * @param childNames  the simple field names within the composite object (e.g., ["lat", "lon"])
     * @param childColumns the corresponding FieldColumn for each child
     * @param docIndex    the document index
     */
    public static ColumnValueXContentParser forCompositeObject(List<String> childNames, List<FieldColumn> childColumns, int docIndex) {
        return new ColumnValueXContentParser(null, docIndex, childNames, childColumns);
    }

    /**
     * Create a parser for a null value.
     */
    public static ColumnValueXContentParser forNullValue() {
        return new ColumnValueXContentParser(null, -1, null, null);
    }

    // --- Leaf scalar mode ---
    private final FieldColumn column;
    private final int docIndex;

    // --- Composite object mode ---
    private final List<String> childNames;
    private final List<FieldColumn> childColumns;

    // State machine
    private Token currentToken;
    private boolean closed;

    // For composite object mode: tracks position in the synthesized token stream
    private int compositePos; // -1 = before START_OBJECT, 0..2*N-1 = field name/value pairs, 2*N = END_OBJECT
    private String currentFieldName;

    private ColumnValueXContentParser(FieldColumn column, int docIndex, List<String> childNames, List<FieldColumn> childColumns) {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, RestApiVersion.current());
        this.column = column;
        this.docIndex = docIndex;
        this.childNames = childNames;
        this.childColumns = childColumns;
        this.currentToken = null;
        this.closed = false;
        this.compositePos = -1;
        this.currentFieldName = null;
    }

    private boolean isCompositeMode() {
        return childNames != null;
    }

    private boolean isNullMode() {
        return column == null && childNames == null;
    }

    private boolean isLeafMode() {
        return column != null && childNames == null;
    }

    @Override
    public Token nextToken() throws IOException {
        if (closed) return null;

        if (isNullMode()) {
            if (currentToken == null) {
                currentToken = Token.VALUE_NULL;
                return currentToken;
            }
            currentToken = null;
            return null;
        }

        if (isCompositeMode()) {
            return nextCompositeToken();
        }

        // Leaf mode: first call returns the value token, second returns null
        if (currentToken == null) {
            currentToken = leafToken();
            return currentToken;
        }
        currentToken = null;
        return null;
    }

    private Token nextCompositeToken() {
        compositePos++;
        int totalEntries = childNames.size();

        if (compositePos == 0) {
            // START_OBJECT
            currentToken = Token.START_OBJECT;
            currentFieldName = null;
            return currentToken;
        }

        int entryIndex = (compositePos - 1) / 2;
        boolean isFieldName = (compositePos - 1) % 2 == 0;

        if (entryIndex < totalEntries) {
            if (isFieldName) {
                currentToken = Token.FIELD_NAME;
                currentFieldName = childNames.get(entryIndex);
                return currentToken;
            } else {
                // Value token
                FieldColumn childCol = childColumns.get(entryIndex);
                if (childCol.isNull(docIndex)) {
                    currentToken = Token.VALUE_NULL;
                } else {
                    currentToken = columnTypeToToken(childCol.columnType());
                }
                return currentToken;
            }
        }

        // END_OBJECT
        currentToken = Token.END_OBJECT;
        currentFieldName = null;
        return currentToken;
    }

    private Token leafToken() {
        if (column.isNull(docIndex)) {
            return Token.VALUE_NULL;
        }
        return columnTypeToToken(column.columnType());
    }

    private static Token columnTypeToToken(ColumnType type) {
        return switch (type) {
            case INT, LONG -> Token.VALUE_NUMBER;
            case STRING -> Token.VALUE_STRING;
            case BOOLEAN -> Token.VALUE_BOOLEAN;
            case BINARY -> Token.VALUE_EMBEDDED_OBJECT;
            case NULL -> Token.VALUE_NULL;
        };
    }

    @Override
    public Token currentToken() {
        return currentToken;
    }

    @Override
    public String currentName() throws IOException {
        if (isCompositeMode() && currentFieldName != null) {
            return currentFieldName;
        }
        if (isLeafMode()) {
            return column.fieldPath();
        }
        return null;
    }

    @Override
    public void skipChildren() throws IOException {
        if (isCompositeMode() && currentToken == Token.START_OBJECT) {
            // Skip to END_OBJECT
            compositePos = 1 + childNames.size() * 2;
            currentToken = Token.END_OBJECT;
            currentFieldName = null;
        }
    }

    @Override
    public String text() throws IOException {
        if (isCompositeMode()) {
            return currentCompositeText();
        }
        if (column == null) return null;

        return switch (column.columnType()) {
            case STRING -> column.stringValue(docIndex);
            case INT -> Integer.toString(column.intValue(docIndex));
            case LONG -> Long.toString(column.longValue(docIndex));
            case BOOLEAN -> Boolean.toString(column.booleanValue(docIndex));
            default -> null;
        };
    }

    private String currentCompositeText() throws IOException {
        int entryIndex = (compositePos - 1) / 2;
        if (entryIndex < 0 || entryIndex >= childColumns.size()) return null;
        FieldColumn childCol = childColumns.get(entryIndex);
        if (childCol.isNull(docIndex)) return null;

        return switch (childCol.columnType()) {
            case STRING -> childCol.stringValue(docIndex);
            case INT -> Integer.toString(childCol.intValue(docIndex));
            case LONG -> Long.toString(childCol.longValue(docIndex));
            case BOOLEAN -> Boolean.toString(childCol.booleanValue(docIndex));
            default -> null;
        };
    }

    @Override
    public XContentString optimizedText() throws IOException {
        FieldColumn col = currentValueColumn();
        if (col != null && col.columnType() == ColumnType.STRING && col.isNull(docIndex) == false) {
            XContentString.UTF8Bytes bytes = col.stringBytes(docIndex);
            if (bytes != null) {
                return new Text(bytes);
            }
        }
        return super.optimizedText();
    }

    private FieldColumn currentValueColumn() {
        if (isCompositeMode()) {
            int entryIndex = (compositePos - 1) / 2;
            if (entryIndex >= 0 && entryIndex < childColumns.size()) {
                return childColumns.get(entryIndex);
            }
            return null;
        }
        return column;
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        String t = text();
        return t != null ? CharBuffer.wrap(t) : null;
    }

    @Override
    public boolean hasTextCharacters() {
        return false;
    }

    @Override
    public char[] textCharacters() throws IOException {
        String t = text();
        return t != null ? t.toCharArray() : new char[0];
    }

    @Override
    public int textLength() throws IOException {
        String t = text();
        return t != null ? t.length() : 0;
    }

    @Override
    public int textOffset() throws IOException {
        return 0;
    }

    @Override
    public Number numberValue() throws IOException {
        FieldColumn col = currentValueColumn();
        if (col == null) return null;
        return switch (col.columnType()) {
            case INT -> col.intValue(docIndex);
            case LONG -> col.longValue(docIndex);
            default -> null;
        };
    }

    @Override
    public NumberType numberType() throws IOException {
        FieldColumn col = currentValueColumn();
        if (col == null) return null;
        return switch (col.columnType()) {
            case INT -> NumberType.INT;
            case LONG -> NumberType.LONG;
            default -> null;
        };
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        FieldColumn col = currentValueColumn();
        if (col == null) return false;
        return col.booleanValue(docIndex);
    }

    @Override
    protected short doShortValue() throws IOException {
        FieldColumn col = currentValueColumn();
        if (col == null) return 0;
        return (short) col.intValue(docIndex);
    }

    @Override
    protected int doIntValue() throws IOException {
        FieldColumn col = currentValueColumn();
        if (col == null) return 0;
        if (col.columnType() == ColumnType.INT) {
            return col.intValue(docIndex);
        }
        return (int) col.longValue(docIndex);
    }

    @Override
    protected long doLongValue() throws IOException {
        FieldColumn col = currentValueColumn();
        if (col == null) return 0;
        if (col.columnType() == ColumnType.LONG) {
            return col.longValue(docIndex);
        }
        return col.intValue(docIndex);
    }

    @Override
    protected float doFloatValue() throws IOException {
        FieldColumn col = currentValueColumn();
        if (col == null) return 0;
        if (col.columnType() == ColumnType.INT) {
            return col.floatValue(docIndex);
        }
        return (float) col.doubleValue(docIndex);
    }

    @Override
    protected double doDoubleValue() throws IOException {
        FieldColumn col = currentValueColumn();
        if (col == null) return 0;
        if (col.columnType() == ColumnType.LONG) {
            return col.doubleValue(docIndex);
        }
        return col.intValue(docIndex);
    }

    @Override
    public Object objectText() throws IOException {
        return objectBytes();
    }

    @Override
    public Object objectBytes() throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) return text();
        if (token == Token.VALUE_NUMBER) return numberValue();
        if (token == Token.VALUE_BOOLEAN) return booleanValue();
        if (token == Token.VALUE_NULL) return null;
        if (token == Token.VALUE_EMBEDDED_OBJECT) return binaryValue();
        return null;
    }

    @Override
    public byte[] binaryValue() throws IOException {
        FieldColumn col = currentValueColumn();
        if (col == null) return null;
        BytesReference ref = col.binaryValue(docIndex);
        return ref != null ? BytesReference.toBytes(ref) : null;
    }

    @Override
    public XContentLocation getTokenLocation() {
        return new XContentLocation(0, 0);
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        // no-op
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }
}
