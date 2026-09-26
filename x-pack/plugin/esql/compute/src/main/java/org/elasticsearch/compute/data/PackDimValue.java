/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * Reusable borrowed accessor for a packed dimension value, like TDigestHolder.
 * It owns no storage and is valid only while its backing block remains open. Representation-specific
 * offsets and dictionary IDs do not escape this holder. Leaf encoding remains private to the dimension codec.
 */
public final class PackDimValue {
    private BytesRefBlock names;
    private BytesRefBlock values;
    private int firstName;
    private int firstValue;
    private int size;

    PackDimValue reset(BytesRefBlock names, BytesRefBlock values, int record) {
        this.names = names;
        this.values = values;
        this.firstName = names.getFirstValueIndex(record);
        this.firstValue = values.getFirstValueIndex(record);
        this.size = names.getValueCount(record);
        return this;
    }

    public int size() {
        return size;
    }

    /** Reads a literal name in canonical UTF-8 order. Copy bytes before retaining beyond this view's lifetime. */
    public BytesRef nameAt(int field, BytesRef scratch) {
        checkField(field);
        return names.getBytesRef(firstName + field, scratch);
    }

    /** Reads a canonical encoded leaf, independent of dictionary layout or storage compression. */
    public BytesRef valueAt(int field, BytesRef scratch) {
        checkField(field);
        return values.getBytesRef(firstValue + field, scratch);
    }

    /** Structural presence, including a present null or empty string. */
    public boolean contains(BytesRef name) {
        return find(name) >= 0;
    }

    /** Returns null only for absence; a present null has a non-null encoded leaf. */
    public BytesRef get(BytesRef name, BytesRef scratch) {
        int field = find(name);
        return field < 0 ? null : valueAt(field, scratch);
    }

    private int find(BytesRef name) {
        int low = 0;
        int high = size - 1;
        BytesRef scratch = new BytesRef();
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int comparison = nameAt(mid, scratch).compareTo(name);
            if (comparison < 0) low = mid + 1;
            else if (comparison > 0) high = mid - 1;
            else return mid;
        }
        return -1;
    }

    private void checkField(int field) {
        if (field < 0 || field >= size) throw new IndexOutOfBoundsException(field);
    }
}
