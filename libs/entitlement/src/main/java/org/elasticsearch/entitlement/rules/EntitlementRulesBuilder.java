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

/**
 * Entry point builder for defining entitlement rules.
 * <p>
 * This builder provides methods to start defining entitlement rules for specific classes.
 * It serves as the top-level entry point in the fluent API for creating entitlement
 * rule definitions.
 * <p>
 * Example usage:
 * <pre>{@code
 * EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);
 * builder.on(File.class)
 *     .calling(File::exists)
 *     .enforce(Policies::fileRead)
 *     .elseThrowNotEntitled();
 * }</pre>
 */
public class EntitlementRulesBuilder {
    private final InternalInstrumentationRegistry registry;

    public EntitlementRulesBuilder(InternalInstrumentationRegistry registry) {
        this.registry = registry;
    }

    /**
     * Starts defining rules for a class specified by name.
     * <p>
     * This method is useful when the class may not be available at compile time
     * or when working with dynamically loaded classes.
     *
     * @param <T> the type of the class
     * @param className the fully qualified name of the class
     * @param publicType the public type/interface used for type safety
     * @return a class method builder for the specified class
     * @throws IllegalArgumentException if the class cannot be found
     */
    @SuppressWarnings("unchecked")
    public <T> ClassMethodBuilder<T> on(String className, Class<? extends T> publicType) {
        try {
            return new ClassMethodBuilder<>(registry, (Class<T>) Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class not found: " + className, e);
        }
    }

    /**
     * Starts defining rules for a class specified by name and applies the specified configuration.
     * <p>
     * This method is useful when the class may not be available at compile time
     * or when working with dynamically loaded classes.
     *
     * @param <T> the type of the class
     * @param className the fully qualified name of the class
     * @param publicType the public type/interface used for type safety
     * @param builderConsumer a consumer that configures the rules for the class
     * @return a class method builder for the specified class
     * @throws IllegalArgumentException if the class cannot be found
     */
    public <T> ClassMethodBuilder<T> on(String className, Class<? extends T> publicType, Consumer<ClassMethodBuilder<T>> builderConsumer) {
        ClassMethodBuilder<T> classMethodBuilder = on(className, publicType);
        builderConsumer.accept(classMethodBuilder);
        return classMethodBuilder;
    }

    /**
     * Starts defining rules for the specified class.
     *
     * @param <T> the type of the class
     * @param clazz the class to define rules for
     * @return a class method builder for the specified class
     */
    public <T> ClassMethodBuilder<T> on(Class<? extends T> clazz) {
        return new ClassMethodBuilder<>(registry, clazz);
    }

    /**
     * Starts defining rules for the specified class and applies the specified configuration.
     *
     * @param <T> the type of the class
     * @param clazz the class to define rules for
     * @param builderConsumer a consumer that configures the rules for the class
     * @return a class method builder for the specified class
     */
    public <T> ClassMethodBuilder<T> on(Class<? extends T> clazz, Consumer<ClassMethodBuilder<T>> builderConsumer) {
        ClassMethodBuilder<T> classMethodBuilder = new ClassMethodBuilder<>(registry, clazz);
        builderConsumer.accept(classMethodBuilder);
        return classMethodBuilder;
    }

    /**
     * Applies the same rule configuration to multiple classes.
     * <p>
     * This method is useful when multiple classes share the same entitlement
     * requirements and rule definitions.
     *
     * @param <T> the common type of the classes
     * @param classes the list of classes to apply rules to
     * @param builderConsumer a consumer that configures the rules for each class
     */
    public <T> void on(List<? extends Class<? extends T>> classes, Consumer<ClassMethodBuilder<T>> builderConsumer) {
        classes.forEach(clazz -> builderConsumer.accept(on(clazz)));
    }
}
