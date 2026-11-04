/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.elasticsearch.compute.data.ElementType;

import java.util.List;

/**
 * Describes how rows of a view index are folded on merge: rows with equal values for all {@link #keys} form a group,
 * and each {@link ReducedField} declares how the group's values are combined into the single surviving row. Every
 * other field keeps the value of the group's representative row, which is what dimensions want.
 *
 * @param keys   single-valued doc values fields that together identify a group: numeric ({@link ElementType#LONG})
 *               or keyword ({@link ElementType#BYTES_REF}) fields, normally the index sort fields
 * @param fields fields whose values are reduced; the stored encoding follows the ES number mappers
 *               (doubles as sortable longs)
 */
public record FoldSpec(List<KeyField> keys, List<ReducedField> fields) {

    public enum Reduction {
        SUM
    }

    /**
     * @param name doc values field name
     * @param type {@link ElementType#LONG} for numeric doc values, {@link ElementType#BYTES_REF} for keyword ordinals
     */
    public record KeyField(String name, ElementType type) {
        public KeyField {
            if (type != ElementType.LONG && type != ElementType.BYTES_REF) {
                throw new IllegalArgumentException("unsupported key field type [" + type + "] for [" + name + "]");
            }
        }
    }

    /**
     * @param name      doc values field name
     * @param type      how the doc values are decoded: {@link ElementType#DOUBLE} (sortable long encoded),
     *                  {@link ElementType#INT} or {@link ElementType#LONG}
     * @param reduction how values of a group are combined
     */
    public record ReducedField(String name, ElementType type, Reduction reduction) {
        public ReducedField {
            if (type != ElementType.DOUBLE && type != ElementType.INT && type != ElementType.LONG) {
                throw new IllegalArgumentException("unsupported reduced field type [" + type + "] for [" + name + "]");
            }
        }
    }

    public FoldSpec {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("a fold needs at least one key field");
        }
        for (ReducedField f : fields) {
            for (KeyField k : keys) {
                if (f.name().equals(k.name())) {
                    throw new IllegalArgumentException("the fold key [" + k.name() + "] cannot be reduced");
                }
            }
        }
    }

    /** A fold with a single numeric key. */
    public FoldSpec(String keyField, List<ReducedField> fields) {
        this(List.of(new KeyField(keyField, ElementType.LONG)), fields);
    }

    /** Position of {@code name} in {@link #fields}, or -1 when the field is not reduced. */
    public int indexOf(String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
