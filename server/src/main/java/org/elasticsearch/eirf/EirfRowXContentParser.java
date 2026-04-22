/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * An {@link org.elasticsearch.xcontent.XContentParser} that walks a pre-built {@link SchemaNode} tree,
 * reading values directly from an {@link EirfRowReader} without intermediate allocations.
 *
 * <p>The {@link SchemaNode} tree is built once per batch from the {@link EirfSchema} and reused
 * across all rows. Each parser instance holds a reference to a specific row's data.
 *
 * <p>The parser emits tokens by walking the tree depth-first: for each object node it emits
 * {@code FIELD_NAME, START_OBJECT}, then recurses into children, then {@code END_OBJECT}.
 * For leaf nodes it emits {@code FIELD_NAME} then the value token.
 */
public final class EirfRowXContentParser extends AbstractXContentParser {

    /**
     * A node in the schema tree. Built once per batch from {@link EirfSchema}.
     * Object nodes have children; leaf nodes have a column index into the row data.
     */
    public static final class SchemaNode {
        final String name;
        final int leafColumnIndex; // -1 for object nodes
        final SchemaNode[] children; // null for leaf nodes

        private SchemaNode(String name, int leafColumnIndex, SchemaNode[] children) {
            this.name = name;
            this.leafColumnIndex = leafColumnIndex;
            this.children = children;
        }

        static SchemaNode leaf(String name, int columnIndex) {
            return new SchemaNode(name, columnIndex, null);
        }

        static SchemaNode object(String name, SchemaNode[] children) {
            return new SchemaNode(name, -1, children);
        }

        boolean isLeaf() {
            return children == null;
        }
    }

    /**
     * Builds a schema tree from an {@link EirfSchema}. Call once per batch, reuse across rows.
     */
    public static SchemaNode buildSchemaTree(EirfSchema schema) {
        // Group children by parent index in a single pass over each level
        int nonLeafCount = schema.nonLeafCount();
        int leafCount = schema.leafCount();

        @SuppressWarnings("unchecked")
        List<Integer>[] nonLeafChildren = new List[nonLeafCount];
        for (int i = 1; i < nonLeafCount; i++) {
            int parent = schema.getNonLeafParent(i);
            if (nonLeafChildren[parent] == null) {
                nonLeafChildren[parent] = new ArrayList<>();
            }
            nonLeafChildren[parent].add(i);
        }

        @SuppressWarnings("unchecked")
        List<Integer>[] leafChildren = new List[nonLeafCount];
        for (int leafIdx = 0; leafIdx < leafCount; leafIdx++) {
            int parent = schema.getLeafParent(leafIdx);
            if (leafChildren[parent] == null) {
                leafChildren[parent] = new ArrayList<>();
            }
            leafChildren[parent].add(leafIdx);
        }

        return buildObjectNode("", 0, schema, nonLeafChildren, leafChildren);
    }

    private static SchemaNode buildObjectNode(
        String name,
        int nonLeafIdx,
        EirfSchema schema,
        List<Integer>[] nonLeafChildren,
        List<Integer>[] leafChildren
    ) {
        List<SchemaNode> childList = new ArrayList<>();

        List<Integer> objectChildren = nonLeafChildren[nonLeafIdx];
        if (objectChildren != null) {
            for (int childIdx : objectChildren) {
                childList.add(buildObjectNode(schema.getNonLeafName(childIdx), childIdx, schema, nonLeafChildren, leafChildren));
            }
        }

        List<Integer> leafChildList = leafChildren[nonLeafIdx];
        if (leafChildList != null) {
            for (int leafIdx : leafChildList) {
                childList.add(SchemaNode.leaf(schema.getLeafName(leafIdx), leafIdx));
            }
        }

        return SchemaNode.object(name, childList.toArray(new SchemaNode[0]));
    }

    // Tree and row data
    private final SchemaNode root;
    private final EirfRowReader row;

    // Walk state: stack of (node, childIndex) pairs
    private SchemaNode[] nodeStack = new SchemaNode[16];
    private int[] childIdxStack = new int[16];
    private int stackDepth;

    // Current state
    private Token currentToken;
    private String currentName;
    private boolean closed;

    // Cached value for current leaf
    private byte currentType;
    private int cachedInt;
    private long cachedLong;
    private float cachedFloat;
    private double cachedDouble;
    private Text cachedText;
    private BytesRef cachedBinary;

    // Pending child to emit content for after FIELD_NAME
    private SchemaNode pendingChild;

    // Compound iteration stack (arrays and key-value objects within arrays)
    private static final byte COMPOUND_ARRAY = 0;
    private static final byte COMPOUND_KV = 1;
    private Object[] compoundReaders = new Object[4]; // EirfArray or EirfKeyValue
    private byte[] compoundTypes = new byte[4];
    private boolean[] compoundNeedValue = new boolean[4]; // for KV frames: true after emitting FIELD_NAME
    private int compoundDepth;

    // Whether we've emitted the root START_OBJECT yet
    private boolean started;

    public EirfRowXContentParser(SchemaNode root, EirfRowReader row) {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, RestApiVersion.current());
        this.root = root;
        this.row = row;
    }

    @Override
    public Token nextToken() throws IOException {
        if (closed) {
            return null;
        }

        if (compoundDepth > 0) {
            return nextCompoundToken();
        }

        // Emit root START_OBJECT first
        if (started == false) {
            started = true;
            push(root);
            currentToken = Token.START_OBJECT;
            return currentToken;
        }

        // If we have a pending child (just emitted its FIELD_NAME), emit its content
        if (pendingChild != null) {
            SchemaNode child = pendingChild;
            pendingChild = null;
            if (child.isLeaf()) {
                return emitLeafValue(child.leafColumnIndex);
            } else {
                push(child);
                currentToken = Token.START_OBJECT;
                return currentToken;
            }
        }

        // Walk the tree
        return walkNext();
    }

    private Token walkNext() {
        while (stackDepth > 0) {
            SchemaNode node = nodeStack[stackDepth - 1];
            int childIdx = childIdxStack[stackDepth - 1];

            if (childIdx < node.children.length) {
                SchemaNode child = node.children[childIdx];
                childIdxStack[stackDepth - 1]++;

                // Skip null leaves
                if (child.isLeaf()) {
                    int colIdx = child.leafColumnIndex;
                    if (colIdx >= row.columnCount() || row.isNull(colIdx)) {
                        continue;
                    }
                }

                // Emit FIELD_NAME, defer content to next call
                currentName = child.name;
                currentToken = Token.FIELD_NAME;
                pendingChild = child;
                return currentToken;
            } else {
                // All children visited — pop and emit END_OBJECT
                pop();
                currentToken = Token.END_OBJECT;
                return currentToken;
            }
        }

        // Stack empty — done
        currentToken = null;
        return null;
    }

    private Token emitLeafValue(int colIdx) {
        currentType = row.getTypeByte(colIdx);
        switch (currentType) {
            case EirfType.INT -> {
                cachedInt = row.getIntValue(colIdx);
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.LONG -> {
                cachedLong = row.getLongValue(colIdx);
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.FLOAT -> {
                cachedFloat = row.getFloatValue(colIdx);
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.DOUBLE -> {
                cachedDouble = row.getDoubleValue(colIdx);
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.STRING -> {
                cachedText = row.getStringValue(colIdx);
                currentToken = Token.VALUE_STRING;
            }
            case EirfType.TRUE, EirfType.FALSE -> currentToken = Token.VALUE_BOOLEAN;
            case EirfType.BINARY -> {
                cachedBinary = row.getBinaryValue(colIdx);
                currentToken = Token.VALUE_EMBEDDED_OBJECT;
            }
            case EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> {
                pushCompound(row.getArrayValue(colIdx), COMPOUND_ARRAY);
                currentToken = Token.START_ARRAY;
            }
            default -> currentToken = Token.VALUE_NULL;
        }
        return currentToken;
    }

    private Token nextCompoundToken() {
        byte frameType = compoundTypes[compoundDepth - 1];
        if (frameType == COMPOUND_ARRAY) {
            return nextArrayFrameToken();
        } else {
            return nextKvFrameToken();
        }
    }

    private Token nextArrayFrameToken() {
        EirfArrayReader reader = (EirfArrayReader) compoundReaders[compoundDepth - 1];
        if (reader.next()) {
            return emitArrayElementValue(reader);
        } else {
            popCompound();
            currentToken = Token.END_ARRAY;
            return currentToken;
        }
    }

    private Token emitArrayElementValue(EirfArrayReader reader) {
        byte elemType = reader.type();
        switch (elemType) {
            case EirfType.INT -> {
                currentType = EirfType.INT;
                cachedInt = reader.intValue();
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.LONG -> {
                currentType = EirfType.LONG;
                cachedLong = reader.longValue();
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.FLOAT -> {
                currentType = EirfType.FLOAT;
                cachedFloat = reader.floatValue();
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.DOUBLE -> {
                currentType = EirfType.DOUBLE;
                cachedDouble = reader.doubleValue();
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.STRING -> {
                currentType = EirfType.STRING;
                cachedText = new Text(reader.stringValue());
                currentToken = Token.VALUE_STRING;
            }
            case EirfType.TRUE, EirfType.FALSE -> {
                currentType = elemType;
                currentToken = Token.VALUE_BOOLEAN;
            }
            case EirfType.NULL -> {
                currentType = EirfType.NULL;
                currentToken = Token.VALUE_NULL;
            }
            case EirfType.KEY_VALUE -> {
                pushCompound(reader.nestedKeyValue(), COMPOUND_KV);
                currentToken = Token.START_OBJECT;
            }
            case EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> {
                pushCompound(reader.nestedArray(), COMPOUND_ARRAY);
                currentToken = Token.START_ARRAY;
            }
            default -> throw new IllegalStateException("Unexpected array element type: " + elemType);
        }
        return currentToken;
    }

    private Token nextKvFrameToken() {
        EirfKeyValueReader kv = (EirfKeyValueReader) compoundReaders[compoundDepth - 1];
        if (compoundNeedValue[compoundDepth - 1]) {
            compoundNeedValue[compoundDepth - 1] = false;
            return emitKvValue(kv);
        }
        if (kv.next()) {
            currentName = kv.key();
            currentToken = Token.FIELD_NAME;
            compoundNeedValue[compoundDepth - 1] = true;
        } else {
            popCompound();
            currentToken = Token.END_OBJECT;
        }
        return currentToken;
    }

    private Token emitKvValue(EirfKeyValueReader kv) {
        byte kvType = kv.type();
        switch (kvType) {
            case EirfType.INT -> {
                currentType = EirfType.INT;
                cachedInt = kv.intValue();
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.LONG -> {
                currentType = EirfType.LONG;
                cachedLong = kv.longValue();
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.FLOAT -> {
                currentType = EirfType.FLOAT;
                cachedFloat = kv.floatValue();
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.DOUBLE -> {
                currentType = EirfType.DOUBLE;
                cachedDouble = kv.doubleValue();
                currentToken = Token.VALUE_NUMBER;
            }
            case EirfType.STRING -> {
                currentType = EirfType.STRING;
                cachedText = new Text(kv.stringValue());
                currentToken = Token.VALUE_STRING;
            }
            case EirfType.TRUE, EirfType.FALSE -> {
                currentType = kvType;
                currentToken = Token.VALUE_BOOLEAN;
            }
            case EirfType.NULL -> {
                currentType = EirfType.NULL;
                currentToken = Token.VALUE_NULL;
            }
            case EirfType.KEY_VALUE -> {
                pushCompound(kv.nestedKeyValue(), COMPOUND_KV);
                currentToken = Token.START_OBJECT;
            }
            case EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> {
                pushCompound(kv.nestedArray(), COMPOUND_ARRAY);
                currentToken = Token.START_ARRAY;
            }
            default -> throw new IllegalStateException("Unexpected key-value type: " + kvType);
        }
        return currentToken;
    }

    private void pushCompound(Object reader, byte type) {
        if (compoundDepth >= compoundReaders.length) {
            int newLen = compoundReaders.length * 2;
            Object[] newReaders = new Object[newLen];
            byte[] newTypes = new byte[newLen];
            boolean[] newNeedValue = new boolean[newLen];
            System.arraycopy(compoundReaders, 0, newReaders, 0, compoundDepth);
            System.arraycopy(compoundTypes, 0, newTypes, 0, compoundDepth);
            System.arraycopy(compoundNeedValue, 0, newNeedValue, 0, compoundDepth);
            compoundReaders = newReaders;
            compoundTypes = newTypes;
            compoundNeedValue = newNeedValue;
        }
        compoundReaders[compoundDepth] = reader;
        compoundTypes[compoundDepth] = type;
        compoundNeedValue[compoundDepth] = false;
        compoundDepth++;
    }

    private void popCompound() {
        compoundDepth--;
        compoundReaders[compoundDepth] = null;
    }

    private void push(SchemaNode node) {
        if (stackDepth >= nodeStack.length) {
            int newLen = nodeStack.length * 2;
            SchemaNode[] newNodes = new SchemaNode[newLen];
            int[] newIdx = new int[newLen];
            System.arraycopy(nodeStack, 0, newNodes, 0, stackDepth);
            System.arraycopy(childIdxStack, 0, newIdx, 0, stackDepth);
            nodeStack = newNodes;
            childIdxStack = newIdx;
        }
        nodeStack[stackDepth] = node;
        childIdxStack[stackDepth] = 0;
        stackDepth++;
    }

    private void pop() {
        stackDepth--;
        nodeStack[stackDepth] = null;
    }

    @Override
    public Token currentToken() {
        return currentToken;
    }

    @Override
    public String currentName() {
        return currentName;
    }

    @Override
    public String text() {
        if (currentToken == null || currentToken.isValue() == false) {
            throw new IllegalArgumentException("Expected text at " + getTokenLocation() + " but found " + currentToken);
        }
        return switch (currentToken) {
            case VALUE_STRING -> cachedText.string();
            case VALUE_NUMBER -> switch (currentType) {
                case EirfType.INT -> Integer.toString(cachedInt);
                case EirfType.LONG -> Long.toString(cachedLong);
                case EirfType.FLOAT -> Float.toString(cachedFloat);
                case EirfType.DOUBLE -> Double.toString(cachedDouble);
                default -> throw new IllegalStateException("Unexpected number type: " + currentType);
            };
            case VALUE_BOOLEAN -> Boolean.toString(currentType == EirfType.TRUE);
            case VALUE_NULL -> "null";
            case VALUE_EMBEDDED_OBJECT -> "";
            default -> throw new IllegalStateException("Unexpected token: " + currentToken);
        };
    }

    @Override
    public CharBuffer charBuffer() {
        return CharBuffer.wrap(text());
    }

    @Override
    public Object objectText() {
        return switch (currentToken) {
            case VALUE_STRING -> text();
            case VALUE_NUMBER -> numberValue();
            case VALUE_BOOLEAN -> doBooleanValue();
            case VALUE_NULL -> null;
            default -> text();
        };
    }

    @Override
    public Object objectBytes() {
        return switch (currentToken) {
            case VALUE_STRING -> charBuffer();
            case VALUE_NUMBER -> numberValue();
            case VALUE_BOOLEAN -> doBooleanValue();
            case VALUE_NULL -> null;
            default -> charBuffer();
        };
    }

    @Override
    public XContentString optimizedText() {
        if (currentToken.isValue() == false) {
            throw new IllegalArgumentException("Expected text at " + getTokenLocation() + " but found " + currentToken);
        }
        if (currentType == EirfType.STRING) {
            return cachedText;
        }
        return new Text(text());
    }

    @Override
    public boolean hasTextCharacters() {
        return false;
    }

    @Override
    public char[] textCharacters() {
        return text().toCharArray();
    }

    @Override
    public int textLength() {
        return text().length();
    }

    @Override
    public int textOffset() {
        return 0;
    }

    @Override
    public Number numberValue() {
        return switch (currentType) {
            case EirfType.INT -> cachedInt;
            case EirfType.LONG -> cachedLong;
            case EirfType.FLOAT -> cachedFloat;
            case EirfType.DOUBLE -> cachedDouble;
            default -> throw new IllegalStateException("Unexpected type: " + currentType);
        };
    }

    @Override
    public NumberType numberType() {
        return switch (currentType) {
            case EirfType.INT -> NumberType.INT;
            case EirfType.LONG -> NumberType.LONG;
            case EirfType.FLOAT -> NumberType.FLOAT;
            case EirfType.DOUBLE -> NumberType.DOUBLE;
            default -> throw new IllegalStateException("Unexpected type: " + currentType);
        };
    }

    @Override
    protected boolean doBooleanValue() {
        return currentType == EirfType.TRUE;
    }

    @Override
    protected short doShortValue() {
        return (short) doIntValue();
    }

    @Override
    protected int doIntValue() {
        return switch (currentType) {
            case EirfType.INT -> cachedInt;
            case EirfType.LONG -> (int) cachedLong;
            case EirfType.FLOAT -> (int) cachedFloat;
            case EirfType.DOUBLE -> (int) cachedDouble;
            default -> throw new IllegalStateException("Unexpected type: " + currentType);
        };
    }

    @Override
    protected long doLongValue() {
        return switch (currentType) {
            case EirfType.LONG -> cachedLong;
            case EirfType.INT -> cachedInt;
            case EirfType.FLOAT -> (long) cachedFloat;
            case EirfType.DOUBLE -> (long) cachedDouble;
            default -> throw new IllegalStateException("Unexpected type: " + currentType);
        };
    }

    @Override
    protected float doFloatValue() {
        return switch (currentType) {
            case EirfType.FLOAT -> cachedFloat;
            case EirfType.DOUBLE -> (float) cachedDouble;
            case EirfType.INT -> (float) cachedInt;
            case EirfType.LONG -> (float) cachedLong;
            default -> throw new IllegalStateException("Unexpected type: " + currentType);
        };
    }

    @Override
    protected double doDoubleValue() {
        return switch (currentType) {
            case EirfType.DOUBLE -> cachedDouble;
            case EirfType.FLOAT -> cachedFloat;
            case EirfType.INT -> cachedInt;
            case EirfType.LONG -> (double) cachedLong;
            default -> throw new IllegalStateException("Unexpected type: " + currentType);
        };
    }

    @Override
    public byte[] binaryValue() {
        return BytesRef.deepCopyOf(cachedBinary).bytes;
    }

    @Override
    public XContentLocation getTokenLocation() {
        // TODO: Handle
        return new XContentLocation(0, 0);
    }

    @Override
    public XContentLocation getCurrentLocation() {
        // TODO: Handle
        return new XContentLocation(0, 0);
    }

    @Override
    public XContentType contentType() {
        // TODO: Handle
        return XContentType.JSON;
    }

    @Override
    public void skipChildren() throws IOException {
        if (currentToken == Token.START_OBJECT || currentToken == Token.START_ARRAY) {
            int depth = 1;
            while (depth > 0) {
                Token t = nextToken();
                if (t == null) break;
                if (t == Token.START_OBJECT || t == Token.START_ARRAY) depth++;
                if (t == Token.END_OBJECT || t == Token.END_ARRAY) depth--;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        // no-op
    }
}
