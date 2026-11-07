/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Reads the key components of documents off a segment's doc values. Documents must be requested in increasing
 * order, like any doc values iteration.
 */
final class KeyReaders {

    private KeyReaders() {}

    /** Iterator over one key component; exactly one of the two iterators is set. */
    record Component(String name, @Nullable SortedNumericDocValues longs, @Nullable SortedSetDocValues bytes) {}

    /** Iterators for every key component, or {@code null} when the segment lacks one of the key fields. */
    @Nullable
    static Component[] open(CodecReader reader, FoldSpec spec) throws IOException {
        Component[] result = new Component[spec.keys().size()];
        for (int i = 0; i < result.length; i++) {
            FoldSpec.KeyField key = spec.keys().get(i);
            FieldInfo info = reader.getFieldInfos().fieldInfo(key.name());
            if (info == null || info.getDocValuesType() == DocValuesType.NONE) {
                return null;
            }
            result[i] = switch (key.type()) {
                case LONG -> new Component(key.name(), numericValues(reader, info), null);
                case BYTES_REF -> new Component(key.name(), null, bytesValues(reader, info));
                default -> throw new IllegalArgumentException("unsupported key type [" + key.type() + "]");
            };
        }
        return result;
    }

    /**
     * Reads the document's key into {@code longs} and {@code bytes} (one slot per component, the unused kind is
     * left alone); {@code false} when a component is missing. Byte values stay valid until the next read.
     */
    static boolean read(Component[] components, int doc, long[] longs, BytesRef[] bytes) throws IOException {
        for (int i = 0; i < components.length; i++) {
            Component c = components[i];
            if (c.longs() != null) {
                if (c.longs().advanceExact(doc) == false) {
                    return false;
                }
                requireSingleValue(c.name(), doc, c.longs().docValueCount());
                longs[i] = c.longs().nextValue();
            } else {
                if (c.bytes().advanceExact(doc) == false) {
                    return false;
                }
                requireSingleValue(c.name(), doc, c.bytes().docValueCount());
                bytes[i] = c.bytes().lookupOrd(c.bytes().nextOrd());
            }
        }
        return true;
    }

    private static void requireSingleValue(String name, int doc, int count) {
        if (count != 1) {
            throw new IllegalStateException("fold key [" + name + "] must be single valued, doc [" + doc + "] has [" + count + "] values");
        }
    }

    static SortedNumericDocValues numericValues(CodecReader reader, FieldInfo info) throws IOException {
        return switch (info.getDocValuesType()) {
            case NUMERIC -> DocValues.singleton(reader.getDocValuesReader().getNumeric(info));
            case SORTED_NUMERIC -> reader.getDocValuesReader().getSortedNumeric(info);
            case NONE, BINARY, SORTED, SORTED_SET -> throw new IllegalArgumentException(
                "fold key [" + info.name + "] must have numeric doc values, got [" + info.getDocValuesType() + "]"
            );
        };
    }

    static SortedSetDocValues bytesValues(CodecReader reader, FieldInfo info) throws IOException {
        return switch (info.getDocValuesType()) {
            case SORTED -> DocValues.singleton(reader.getDocValuesReader().getSorted(info));
            case SORTED_SET -> reader.getDocValuesReader().getSortedSet(info);
            case NONE, BINARY, NUMERIC, SORTED_NUMERIC -> throw new IllegalArgumentException(
                "fold key [" + info.name + "] must have keyword doc values, got [" + info.getDocValuesType() + "]"
            );
        };
    }
}
