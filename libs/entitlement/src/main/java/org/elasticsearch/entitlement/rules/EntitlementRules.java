/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class EntitlementRules {
    private static final List<EntitlementRule> RULES = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public static <T> ClassMethodBuilder<T> on(String className, Class<? extends T> publicType) {
        try {
            return new ClassMethodBuilder<>((Class<T>) Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class not found: " + className, e);
        }
    }

    public static <T> ClassMethodBuilder<T> on(Class<? extends T> clazz) {
        return new ClassMethodBuilder<>(clazz);
    }

    public static <T> void on(List<? extends Class<? extends T>> classes, Consumer<ClassMethodBuilder<T>> builderConsumer) {
        classes.forEach(clazz -> builderConsumer.accept(on(clazz)));
    }

    public static List<EntitlementRule> getRules() {
        return RULES;
    }

    public static void registerRule(EntitlementRule rule) {
        RULES.add(rule);
    }
}
