/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.IRTreeVisitor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class IRNode {

    /* ---- begin decorations ---- */

    public abstract static class IRDecoration<V> {

        private final V value;

        public IRDecoration(V value) {
            this.value = Objects.requireNonNull(value);
        }

        public V getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }

    private final Map<Class<? extends IRDecoration<?>>, IRDecoration<?>> decorations = new HashMap<>();

    @SuppressWarnings("unchecked")
    public <V> V attachDecoration(IRDecoration<V> decoration) {
        IRDecoration<V> previous = (IRDecoration<V>)decorations.put((Class<? extends IRDecoration<?>>)decoration.getClass(), decoration);
        return previous == null ? null : previous.getValue();
    }

    public <T extends IRDecoration<?>> T removeDecoration(Class<T> type) {
        return type.cast(decorations.remove(type));
    }

    public <T extends IRDecoration<?>> T getDecoration(Class<T> type) {
        return type.cast(decorations.get(type));
    }

    public <T extends IRDecoration<V>, V> V getDecorationValue(Class<T> type) {
        return getDecorationValueOrDefault(type, null);
    }

    public <T extends IRDecoration<V>, V> V getDecorationValueOrDefault(Class<T> type, V defaultValue) {
        T decoration = type.cast(decorations.get(type));
        return decoration == null ? defaultValue : decoration.getValue();
    }

    public <T extends IRDecoration<?>> String getDecorationString(Class<T> type) {
        T decoration = type.cast(decorations.get(type));
        return decoration == null ? null : decoration.toString();
    }

    /* ---- end decorations, begin conditions ---- */

    public interface IRCondition {

    }

    private final Set<Class<? extends IRCondition>> conditions = new HashSet<>();

    public boolean attachCondition(Class<? extends IRCondition> type) {
        return conditions.add(type);
    }

    public boolean removeCondition(Class<? extends IRCondition> type) {
        return conditions.remove(type);
    }

    public boolean hasCondition(Class<? extends IRCondition> type) {
        return conditions.contains(type);
    }

    /* ---- end conditions, begin node data ---- */

    private final Location location;

    public Location getLocation() {
        return location;
    }

    /* ---- end node data, begin visitor ---- */

    public abstract <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope);
    public abstract <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope);

    /* ---- end visitor ---- */

    public IRNode(Location location) {
        this.location = location;
    }

}
