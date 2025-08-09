/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import java.util.List;
import java.util.Map;

public abstract class ESONEntry {

    public static final byte TYPE_NULL = 0x00;
    public static final byte TYPE_FALSE = 0x01;
    public static final byte TYPE_TRUE = 0x02;
    public static final byte TYPE_INT = 0x03;
    public static final byte TYPE_LONG = 0x04;
    public static final byte TYPE_FLOAT = 0x05;
    public static final byte TYPE_DOUBLE = 0x06;
    public static final byte STRING = 0x07;
    public static final byte BINARY = 0x08;
    public static final byte TYPE_OBJECT = 0x09;
    // TODO: Maybe add fixed width arrays
    public static final byte TYPE_ARRAY = 0x0A;
    public static final byte BIG_INTEGER = 0x0B;
    public static final byte BIG_DECIMAL = 0x0C;
    // TODO: Fix
    public static final byte MUTATION = 0x64;

    private final byte type;
    private final String key;

    ESONEntry(byte type, String key) {
        this.type = type;
        this.key = key;
    }

    public String key() {
        return key;
    }

    public byte type() {
        return type;
    }

    public static class ObjectEntry extends ESONEntry {

        public int fieldCount = 0;
        public Map<String, ESONSource.Value> mutationMap = null;

        public ObjectEntry(String key) {
            super(TYPE_OBJECT, key);
        }

        public boolean hasMutations() {
            return mutationMap != null;
        }

        @Override
        public String toString() {
            return "ObjectEntry{" + "key='" + key() + '\'' + ", fieldCount=" + fieldCount + ", hasMutations=" + hasMutations() + '}';
        }
    }

    public static class ArrayEntry extends ESONEntry {

        public int elementCount = 0;
        public List<ESONSource.Value> mutationArray = null;

        public ArrayEntry(String key) {
            super(TYPE_ARRAY, key);
        }

        public boolean hasMutations() {
            return mutationArray != null;
        }

        @Override
        public String toString() {
            return "ArrayEntry{" + "key='" + key() + '\'' + ", elementCount=" + elementCount + ", hasMutations=" + hasMutations() + '}';
        }
    }

    public static class FieldEntry extends ESONEntry {

        public final ESONSource.Value value;

        public FieldEntry(String key, ESONSource.Value value) {
            super(value.type(), key);
            this.value = value;
        }

        @Override
        public String toString() {
            return "FieldEntry{" + "value=" + value + '}';
        }
    }
}
