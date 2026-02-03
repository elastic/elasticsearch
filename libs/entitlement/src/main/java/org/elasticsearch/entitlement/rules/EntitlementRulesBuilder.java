/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.util.List;
import java.util.function.Consumer;

public class EntitlementRulesBuilder {
    private final InternalInstrumentationRegistry registry;

    public EntitlementRulesBuilder(InternalInstrumentationRegistry registry) {
        this.registry = registry;
    }

    @SuppressWarnings("unchecked")
    public <T> ClassMethodBuilder<T> on(String className, Class<? extends T> publicType) {
        try {
            return new ClassMethodBuilder<>(registry, (Class<T>) Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class not found: " + className, e);
        }
    }

    public <T> ClassMethodBuilder<T> on(Class<? extends T> clazz) {
        return new ClassMethodBuilder<>(registry, clazz);
    }

    public <T> void on(List<? extends Class<? extends T>> classes, Consumer<ClassMethodBuilder<T>> builderConsumer) {
        classes.forEach(clazz -> builderConsumer.accept(on(clazz)));
    }
}
