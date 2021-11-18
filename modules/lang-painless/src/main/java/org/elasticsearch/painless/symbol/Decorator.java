/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.symbol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Decorator {

    public interface Decoration {

    }

    public interface Condition {

    }

    private final ArrayList<Map<Class<? extends Decoration>, Decoration>> decorations;
    private final ArrayList<Set<Class<? extends Condition>>> conditions;

    public Decorator(int nodeCount) {
        decorations = new ArrayList<>(nodeCount);
        conditions = new ArrayList<>(nodeCount);

        for (int i = 0; i < nodeCount; ++i) {
            decorations.add(new HashMap<>());
            conditions.add(new HashSet<>());
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Decoration> T put(int identifier, T decoration) {
        return (T) decorations.get(identifier).put(decoration.getClass(), decoration);
    }

    public <T extends Decoration> T remove(int identifier, Class<T> type) {
        return type.cast(decorations.get(identifier).remove(type));
    }

    public <T extends Decoration> T get(int identifier, Class<T> type) {
        return type.cast(decorations.get(identifier).get(type));
    }

    public boolean has(int identifier, Class<? extends Decoration> type) {
        return decorations.get(identifier).containsKey(type);
    }

    public <T extends Decoration> boolean copy(int originalIdentifier, int targetIdentifier, Class<T> type) {
        T decoration = get(originalIdentifier, type);

        if (decoration != null) {
            put(targetIdentifier, decoration);

            return true;
        }

        return false;
    }

    public boolean set(int identifier, Class<? extends Condition> type) {
        return conditions.get(identifier).add(type);
    }

    public boolean delete(int identifier, Class<? extends Condition> type) {
        return conditions.get(identifier).remove(type);
    }

    public boolean exists(int identifier, Class<? extends Condition> type) {
        return conditions.get(identifier).contains(type);
    }

    public boolean replicate(int originalIdentifier, int targetIdentifier, Class<? extends Condition> type) {
        if (exists(originalIdentifier, type)) {
            set(targetIdentifier, type);

            return true;
        }

        return false;
    }

    public Map<Class<? extends Decoration>, Decoration> getAllDecorations(int identifier) {
        return decorations.get(identifier);
    }

    public Set<Class<? extends Condition>> getAllConditions(int identifier) {
        return conditions.get(identifier);
    }
}
