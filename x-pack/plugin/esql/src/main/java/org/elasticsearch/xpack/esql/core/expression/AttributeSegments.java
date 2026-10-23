/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * Immutable sequence of attributes stored as segments that can be shared: {@link #append} reuses every segment of this
 * sequence and adds one. A chain of sequences that extend each other stores each attribute once, plus one reference per
 * earlier segment in each sequence's segment array, so it costs O(attributes + sequences²) references instead of
 * O(attributes × sequences) for a copy each.
 * <p>
 * Lookups by id ({@link Attribute#semanticEquals}) and by name probe each segment; a segment builds its lookup tables on first
 * use, and every sequence containing it shares them. This is the storage of both {@link AttributeList} and
 * {@link AttributeSet}; the uniqueness rules are theirs to enforce.
 */
final class AttributeSegments extends AbstractList<Attribute> implements RandomAccess {

    static final AttributeSegments EMPTY = new AttributeSegments(new Segment[0]);

    /**
     * Segments of at most this many attributes are scanned instead of indexed.
     */
    static final int LINEAR_SCAN_LIMIT = 8;

    /**
     * 2^32 / golden ratio (Fibonacci hashing): spreads clustered hashes such as sequential name ids across the table.
     */
    private static final int GOLDEN_RATIO = 0x9E3779B9;

    private final Segment[] segments;

    private AttributeSegments(Segment[] segments) {
        this.segments = segments;
    }

    /**
     * A sequence of {@code items}, which it takes ownership of.
     */
    static AttributeSegments of(Attribute[] items) {
        return items.length == 0 ? EMPTY : new AttributeSegments(new Segment[] { new Segment(0, items) });
    }

    /**
     * Whether no two of {@code items} share a name or an id.
     */
    static boolean distinct(Attribute[] items) {
        if (items.length <= LINEAR_SCAN_LIMIT) {
            for (int i = 1; i < items.length; i++) {
                for (int j = 0; j < i; j++) {
                    if (items[i].name().equals(items[j].name()) || items[i].semanticEquals(items[j])) {
                        return false;
                    }
                }
            }
            return true;
        }
        return table(items, item -> item.name().hashCode(), (a, b) -> a.name().equals(b.name())) != null
            && table(items, Attribute::semanticHash, Attribute::semanticEquals) != null;
    }

    private static int slot(int hash, int mask) {
        int mixed = hash * GOLDEN_RATIO;
        return (mixed ^ (mixed >>> 15)) & mask;
    }

    /**
     * An open-addressing table of the positions of {@code items}: slots hold {@code position + 1}, zero is empty. At most half
     * of them are used, so probing stays short. Returns {@code null} if {@code duplicate} matches two items with the same hash.
     */
    private static int[] table(Attribute[] items, ToIntFunction<Attribute> hash, BiPredicate<Attribute, Attribute> duplicate) {
        int[] table = new int[Integer.highestOneBit(items.length * 2 - 1) << 1];
        int mask = table.length - 1;
        for (int i = 0; i < items.length; i++) {
            int slot = slot(hash.applyAsInt(items[i]), mask);
            while (table[slot] != 0) {
                if (duplicate != null && duplicate.test(items[table[slot] - 1], items[i])) {
                    return null;
                }
                slot = (slot + 1) & mask;
            }
            table[slot] = i + 1;
        }
        return table;
    }

    /**
     * This sequence followed by {@code items}, which it takes ownership of.
     */
    AttributeSegments append(Attribute[] items) {
        if (items.length == 0) {
            return this;
        }
        int count = segments.length;
        Segment[] appended = Arrays.copyOf(segments, count + 1);
        appended[count] = new Segment(size(), items);
        return new AttributeSegments(appended);
    }

    /**
     * Whether an attribute semantically equal to {@code attribute} is in this sequence.
     */
    boolean containsAttribute(Attribute attribute) {
        for (Segment segment : segments) {
            if (segment.contains(attribute)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether any attribute in this sequence is called {@code name}.
     */
    boolean containsName(String name) {
        for (Segment segment : segments) {
            if (segment.containsName(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int size() {
        return segments.length == 0 ? 0 : segments[segments.length - 1].end();
    }

    @Override
    public Attribute get(int index) {
        Objects.checkIndex(index, size());
        int low = 0;
        int high = segments.length - 1;
        while (low < high) {
            int mid = (low + high + 1) >>> 1;
            if (segments[mid].start <= index) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        Segment segment = segments[low];
        return segment.items[index - segment.start];
    }

    @Override
    public Iterator<Attribute> iterator() {
        return new Iterator<>() {
            private int segment = 0;
            private int offset = 0;

            @Override
            public boolean hasNext() {
                return segment < segments.length;
            }

            @Override
            public Attribute next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                Attribute[] items = segments[segment].items;
                Attribute next = items[offset++];
                if (offset == items.length) {
                    segment++;
                    offset = 0;
                }
                return next;
            }
        };
    }

    @Override
    public void forEach(Consumer<? super Attribute> action) {
        for (Segment segment : segments) {
            for (Attribute attribute : segment.items) {
                action.accept(attribute);
            }
        }
    }

    @Override
    public Object[] toArray() {
        Object[] all = new Object[size()];
        for (Segment segment : segments) {
            System.arraycopy(segment.items, 0, all, segment.start, segment.items.length);
        }
        return all;
    }

    /**
     * A run of attributes shared by the sequence that added it and every sequence appended from that one. Segments longer than
     * {@link #LINEAR_SCAN_LIMIT} index their attributes by id and by name in open-addressing tables of positions, built on
     * first use. Racing first calls may each build one; they build equal tables and nobody changes one once published.
     */
    private static final class Segment {
        private final int start;
        private final Attribute[] items;
        private volatile int[] idTable;
        private volatile int[] nameTable;

        private Segment(int start, Attribute[] items) {
            this.start = start;
            this.items = items;
        }

        private int end() {
            return start + items.length;
        }

        private boolean contains(Attribute attribute) {
            if (items.length <= LINEAR_SCAN_LIMIT) {
                for (Attribute item : items) {
                    if (item.semanticEquals(attribute)) {
                        return true;
                    }
                }
                return false;
            }
            int[] table = idTable;
            if (table == null) {
                table = table(items, Attribute::semanticHash, null);
                idTable = table;
            }
            int mask = table.length - 1;
            for (int slot = slot(attribute.semanticHash(), mask); table[slot] != 0; slot = (slot + 1) & mask) {
                if (items[table[slot] - 1].semanticEquals(attribute)) {
                    return true;
                }
            }
            return false;
        }

        private boolean containsName(String name) {
            if (items.length <= LINEAR_SCAN_LIMIT) {
                for (Attribute item : items) {
                    if (item.name().equals(name)) {
                        return true;
                    }
                }
                return false;
            }
            int[] table = nameTable;
            if (table == null) {
                table = table(items, item -> item.name().hashCode(), null);
                nameTable = table;
            }
            int mask = table.length - 1;
            for (int slot = slot(name.hashCode(), mask); table[slot] != 0; slot = (slot + 1) & mask) {
                if (items[table[slot] - 1].name().equals(name)) {
                    return true;
                }
            }
            return false;
        }
    }
}
