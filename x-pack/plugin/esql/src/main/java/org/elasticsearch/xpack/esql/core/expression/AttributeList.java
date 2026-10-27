/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.util.FeatureFlag;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.function.Consumer;

/**
 * Immutable list of attributes with distinct names and distinct ids, built so that a command's output can share storage
 * with its child's output.
 * <p>
 * Most plan commands keep their child's output and append a few attributes to it. Copying the child's output into every
 * node, as a list plus the set built from it, costs O(width × depth) entries of tens of bytes each for a long chain of
 * commands. Instead, {@link #merge} reuses the child's {@link AttributeSegments} and adds one segment for the new
 * attributes, so a chain stores each attribute once, plus one reference per ancestor in each node's segment array:
 * O(width + depth²) references, a few bytes each.
 * <p>
 * Checking whether a new attribute collides with the child costs O(new attributes × segments) instead of a scan of the
 * whole child, and {@link AttributeSet#of(Collection)} shares the list's storage instead of copying it. Because names and
 * ids are distinct by construction, nothing downstream has to check an {@link AttributeList} for duplicates again.
 */
public final class AttributeList extends AbstractList<Attribute> implements RandomAccess {

    /**
     * Whether plan nodes share their outputs through {@link AttributeList} and {@link AttributeSet} shares their storage.
     * When disabled, {@link org.elasticsearch.xpack.esql.expression.NamedExpressions#mergeOutputAttributes} copies outputs
     * into plain lists and every {@link AttributeSet} copies its attributes into an {@link AttributeMap}.
     * <p>
     * Short-lived: it keeps the shared storage to snapshot builds so CI, nightlies and tests exercise it before users see
     * it. Once that has been clean, remove the flag and the disabled branches rather than turning it on.
     */
    public static final FeatureFlag SHARED_OUTPUT_FEATURE_FLAG = new FeatureFlag("esql_shared_plan_output");

    public static final AttributeList EMPTY = new AttributeList(AttributeSegments.EMPTY);

    private final AttributeSegments attributes;

    private AttributeList(AttributeSegments attributes) {
        this.attributes = attributes;
    }

    /**
     * Copies {@code attributes} into a new list, or returns it if it is already an {@link AttributeList}.
     *
     * @throws IllegalArgumentException if two attributes share a name or an id
     */
    public static AttributeList of(List<? extends Attribute> attributes) {
        if (attributes instanceof AttributeList list) {
            return list;
        }
        AttributeList copy = distinctCopy(attributes);
        if (copy == null) {
            throw new IllegalArgumentException("duplicate attributes in " + attributes);
        }
        return copy;
    }

    private static AttributeList distinctCopy(List<? extends Attribute> attributes) {
        if (attributes.isEmpty()) {
            return EMPTY;
        }
        Attribute[] items = attributes.toArray(new Attribute[0]);
        return AttributeSegments.distinct(items) ? new AttributeList(AttributeSegments.of(items)) : null;
    }

    /**
     * The output of a command that adds {@code fields} to its child's output, with the semantics of
     * {@link org.elasticsearch.xpack.esql.expression.NamedExpressions#mergeOutputExpressions}: the child's attributes come
     * first, a child attribute whose name a field reuses is dropped, and of fields that repeat a name only the last is kept,
     * at its last position.
     * <p>
     * When {@code childOutput} is an {@link AttributeList} and nothing is dropped, the result shares its storage. Otherwise
     * it is a copy, which is an {@link AttributeList} unless the attributes it holds aren't distinct.
     */
    public static List<Attribute> merge(List<? extends Attribute> fields, List<? extends Attribute> childOutput) {
        List<? extends Attribute> kept = lastOfEachName(fields);
        if (childOutput instanceof AttributeList child) {
            AttributeList appended = child.tryAppend(kept);
            if (appended != null) {
                return appended;
            }
        }
        return mergeByCopy(kept, childOutput);
    }

    private static List<? extends Attribute> lastOfEachName(List<? extends Attribute> fields) {
        if (fields.size() <= 1) {
            return fields;
        }
        Map<String, Integer> lastPositions = new HashMap<>(fields.size() * 2);
        for (int i = 0; i < fields.size(); i++) {
            lastPositions.put(fields.get(i).name(), i);
        }
        if (lastPositions.size() == fields.size()) {
            return fields;
        }
        List<Attribute> kept = new ArrayList<>(lastPositions.size());
        for (int i = 0; i < fields.size(); i++) {
            if (lastPositions.get(fields.get(i).name()) == i) {
                kept.add(fields.get(i));
            }
        }
        return kept;
    }

    private static List<Attribute> mergeByCopy(List<? extends Attribute> kept, List<? extends Attribute> childOutput) {
        AttributeSegments shadowing = AttributeSegments.of(kept.toArray(new Attribute[0]));
        List<Attribute> merged = new ArrayList<>(childOutput.size() + kept.size());
        for (Attribute childAttribute : childOutput) {
            if (shadowing.containsName(childAttribute.name()) == false) {
                merged.add(childAttribute);
            }
        }
        merged.addAll(kept);
        AttributeList copy = distinctCopy(merged);
        return copy != null ? copy : merged;
    }

    /**
     * Returns this list followed by {@code extra}, sharing this list's storage.
     *
     * @throws IllegalArgumentException if an attribute in {@code extra} has the name or id of another attribute in
     * {@code extra} or in this list
     */
    public AttributeList append(List<? extends Attribute> extra) {
        AttributeList appended = tryAppend(extra);
        if (appended == null) {
            throw new IllegalArgumentException("duplicate attributes appending " + extra);
        }
        return appended;
    }

    private AttributeList tryAppend(List<? extends Attribute> extra) {
        if (extra.isEmpty()) {
            return this;
        }
        if (attributes.isEmpty()) {
            return distinctCopy(extra);
        }
        Attribute[] items = extra.toArray(new Attribute[0]);
        if (AttributeSegments.distinct(items) == false) {
            return null;
        }
        for (Attribute item : items) {
            if (containsName(item.name()) || containsAttribute(item)) {
                return null;
            }
        }
        return new AttributeList(attributes.append(items));
    }

    /**
     * Whether any attribute in this list is called {@code name}.
     */
    public boolean containsName(String name) {
        return attributes.containsName(name);
    }

    /**
     * Whether this list contains an attribute semantically equal to {@code o}.
     */
    public boolean containsAttribute(Object o) {
        return o instanceof NamedExpression ne && attributes.containsAttribute(ne.toAttribute());
    }

    /**
     * The storage, for {@link AttributeSet} to share.
     */
    AttributeSegments attributes() {
        return attributes;
    }

    @Override
    public int size() {
        return attributes.size();
    }

    @Override
    public Attribute get(int index) {
        return attributes.get(index);
    }

    @Override
    public Iterator<Attribute> iterator() {
        return attributes.iterator();
    }

    @Override
    public void forEach(Consumer<? super Attribute> action) {
        attributes.forEach(action);
    }

    @Override
    public Object[] toArray() {
        return attributes.toArray();
    }
}
