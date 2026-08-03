/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentString;

import java.util.Arrays;

/** Adapts {@link EscfColumn} data into typed cursors for use by field mappers. */
public final class EscfColumnTransforms {

    private static final BytesRef BOOLEAN_TRUE = new BytesRef("true");
    private static final BytesRef BOOLEAN_FALSE = new BytesRef("false");

    private EscfColumnTransforms() {}

    /**
     * Writes all present values from {@code source} with doc-id &lt; {@code beforeDoc} into
     * {@code dest} via a fresh cursor. No filtering is applied.
     *
     * @throws IllegalArgumentException if {@code beforeDoc} exceeds {@code source.docCount()}
     */
    public static void backfillUtf8Before(EscfColumnBuilder dest, EscfColumn source, int beforeDoc) {
        if (beforeDoc > source.docCount()) {
            throw new IllegalArgumentException("beforeDoc (" + beforeDoc + ") exceeds source docCount (" + source.docCount() + ")");
        }
        // We could always reach down and copy offsets and data directly. We don't need to use cursors.
        final ObjectTupleCursor<BytesRef> replayCursor = utf8Cursor(source);
        for (int d = replayCursor.nextDoc(); d < beforeDoc; d = replayCursor.nextDoc()) {
            dest.setString(d, replayCursor.value());
        }
    }

    /**
     * Returns a cursor that stringifies every present value to its UTF-8 keyword form. Nested and
     * flat arrays are flattened to one tuple per leaf element (same doc-id repeated); absent rows
     * and empty arrays emit nothing. JSON null emits a tuple with {@code value()==null}.
     * Numbers use canonical {@code toString}; BINARY and KEY_VALUE throw.
     *
     * @param column the source ESCF column
     * @return a forward-only cursor over stringified values
     */
    public static ObjectTupleCursor<BytesRef> utf8Cursor(EscfColumn column) {
        // STRING columns have a native BytesRef cursor with no per-row dispatch needed.
        if (column.kind() == EscfColumnKind.STRING) {
            return column.bytesRefCursor();
        }
        return new ObjectTupleCursor<>() {

            private final PresentDocIterator present = column.presentDocs();
            private int currentDoc = -1;
            private BytesRef currentValue = null;
            // Stack of active array readers; grows on nested arrays, shrinks on exhaustion.
            private ArrayReader[] arStack = null;
            private int arDepth = 0;

            @Override
            public int nextDoc() {
                if (arDepth > 0 && advanceArray()) {
                    return currentDoc;
                }
                int doc;
                while ((doc = present.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    currentDoc = doc;
                    byte t = column.typeByteForPresent(doc);
                    switch (t) {
                        case SourceValueType.NULL -> {
                            currentValue = null;
                            return doc;
                        }
                        case SourceValueType.TRUE -> {
                            currentValue = BOOLEAN_TRUE;
                            return doc;
                        }
                        case SourceValueType.FALSE -> {
                            currentValue = BOOLEAN_FALSE;
                            return doc;
                        }
                        case SourceValueType.LONG -> {
                            currentValue = new BytesRef(Long.toString(column.getLongValue(doc)));
                            return doc;
                        }
                        case SourceValueType.DOUBLE -> {
                            currentValue = new BytesRef(Double.toString(column.getDoubleValue(doc)));
                            return doc;
                        }
                        case SourceValueType.STRING -> {
                            currentValue = column.getBinaryValue(doc);
                            return doc;
                        }
                        case SourceValueType.FIXED_ARRAY, SourceValueType.UNION_ARRAY -> {
                            pushArray(column.getArrayValue(doc));
                            if (advanceArray()) {
                                return doc;
                            }
                            // empty array: fall through to the next present doc
                        }
                        // Unsupported scalar type bytes. BINARY and KEY_VALUE are structurally
                        // incompatible with keyword conversion. INT and FLOAT are never written
                        // as scalar UNION type bytes by EscfColumnBuilder (only LONG/DOUBLE are);
                        // they only appear as element type bytes inside inline array payloads.
                        default -> throw new UnsupportedOperationException(
                            "utf8Cursor: unsupported ESCF value type [" + SourceValueType.name(t) + "] for string conversion"
                        );
                    }
                }
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            /** Returns true with {@link #currentValue} set at the next leaf; false when drained. */
            private boolean advanceArray() {
                while (arDepth > 0) {
                    ArrayReader cur = arStack[arDepth - 1];
                    if (cur.next() == false) {
                        arDepth--;
                        continue;
                    }
                    if (cur.isNull()) {
                        currentValue = null;
                        return true;
                    }
                    byte elemType = cur.type();
                    switch (elemType) {
                        case SourceValueType.FIXED_ARRAY, SourceValueType.UNION_ARRAY -> pushArray(cur.nestedArray());
                        case SourceValueType.TRUE -> {
                            currentValue = BOOLEAN_TRUE;
                            return true;
                        }
                        case SourceValueType.FALSE -> {
                            currentValue = BOOLEAN_FALSE;
                            return true;
                        }
                        case SourceValueType.INT -> {
                            currentValue = new BytesRef(Integer.toString(cur.intValue()));
                            return true;
                        }
                        case SourceValueType.LONG -> {
                            currentValue = new BytesRef(Long.toString(cur.longValue()));
                            return true;
                        }
                        case SourceValueType.FLOAT -> {
                            currentValue = new BytesRef(Float.toString(cur.floatValue()));
                            return true;
                        }
                        case SourceValueType.DOUBLE -> {
                            currentValue = new BytesRef(Double.toString(cur.doubleValue()));
                            return true;
                        }
                        case SourceValueType.STRING -> {
                            XContentString.UTF8Bytes text = cur.textValue().bytes();
                            currentValue = new BytesRef(text.bytes(), text.offset(), text.length());
                            return true;
                        }
                        default -> throw new UnsupportedOperationException(
                            "utf8Cursor: unsupported ESCF array element type [" + SourceValueType.name(elemType) + "] for string conversion"
                        );
                    }
                }
                return false;
            }

            private void pushArray(ArrayReader reader) {
                if (arStack == null) {
                    arStack = new ArrayReader[4];
                } else if (arDepth == arStack.length) {
                    arStack = Arrays.copyOf(arStack, arStack.length * 2);
                }
                arStack[arDepth++] = reader;
            }

            @Override
            public BytesRef value() {
                return currentValue;
            }
        };
    }
}
