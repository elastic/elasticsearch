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
    // TODO: Maybe add fixed width arrays
    public static final byte BIG_INTEGER = 0x09;
    public static final byte BIG_DECIMAL = 0x0A;
    public static final byte TYPE_OBJECT = 0x0B;
    public static final byte TYPE_ARRAY = 0x0C;
    // TODO: Fix
    public static final byte MUTATION = 0x64;

    private final byte type;
    private final String key;
    private final ESONSource.Value value;
    private int offsetOrCount = -1;

    ESONEntry(byte type, String key) {
        this(type, key, -1, null);
    }

    ESONEntry(byte type, String key, int offsetOrCount, ESONSource.Value value) {
        this.type = type;
        this.key = key;
        this.offsetOrCount = offsetOrCount;
        this.value = value;
    }

    public String key() {
        return key;
    }

    public byte type() {
        return type;
    }

    public ESONSource.Value value() {
        return value;
    }

    public int offsetOrCount() {
        return offsetOrCount;
    }

    public void offsetOrCount(int offsetOrCount) {
        this.offsetOrCount = offsetOrCount;
    }

    public static class ObjectEntry extends ESONEntry {

        public Map<String, ESONSource.Value> mutationMap = null;

        public ObjectEntry(String key) {
            super(TYPE_OBJECT, key);
        }

        public boolean hasMutations() {
            return mutationMap != null;
        }

        @Override
        public String toString() {
            return "ObjectEntry{" + "type=" + type() + ", key='" + key() + '\'' + ", offsetOrCount=" + offsetOrCount() + '}';
        }
    }

    public static class ArrayEntry extends ESONEntry {

        public List<ESONSource.Value> mutationArray = null;

        public ArrayEntry(String key) {
            super(TYPE_ARRAY, key);
        }

        public boolean hasMutations() {
            return mutationArray != null;
        }

        @Override
        public String toString() {
            return "ArrayEntry{" + "type=" + type() + ", key='" + key() + '\'' + ", offsetOrCount=" + offsetOrCount() + '}';
        }
    }

    public static class FieldEntry extends ESONEntry {

        public FieldEntry(String key, ESONSource.Value value) {
            super(value.type(), key, -1, value);
        }

        public FieldEntry(String key, byte type, int offset) {
            super(type, key, offset, parseValue(type, offset));
        }

        private static ESONSource.Value parseValue(byte type, int offset) {
            return switch (type) {
                case TYPE_NULL -> ESONSource.ConstantValue.NULL;
                case TYPE_FALSE -> ESONSource.ConstantValue.FALSE;
                case TYPE_TRUE -> ESONSource.ConstantValue.TRUE;
                case TYPE_INT, TYPE_DOUBLE, TYPE_FLOAT, TYPE_LONG -> new ESONSource.FixedValue(offset, type);
                case STRING, BINARY, BIG_INTEGER, BIG_DECIMAL -> new ESONSource.VariableValue(offset, type);
                default -> throw new IllegalArgumentException("Unknown type: " + type);
            };
        }

        @Override
        public String toString() {
            return "FieldEntry{"
                + "value="
                + value()
                + ", type="
                + type()
                + ", key='"
                + key()
                + '\''
                + ", offsetOrCount="
                + offsetOrCount()
                + '}';
        }
    }
}
