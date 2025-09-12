/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.injection.guice.TypeLiteral;
import org.elasticsearch.plugins.Plugin;

/**
 * <p>
 * Associates a {@link TypeLiteral} with a {@link Plugin} component instance for dependency injection binding.
 * This can be used when the component type cannot be represented by a simple {@link Class} object,
 * such as parameterized types like {@code List<Authenticator>}.
 * </p>
 * <p>
 * Unlike {@link PluginComponentBinding}, which binds interface-to-implementation relationships,
 * this record handles generic types that require {@link TypeLiteral} for proper type erasure handling.
 * </p>
 *
 * <p>
 * Usage:
 * <pre>{@code
 * public Collection<Object> createComponents(...) {
 *     List<Object> components = new ArrayList<>();
 *     List<Authenticator> authenticators = List.of(...);
 *     components.add(new PluginComponentTypeLiteralBinding<>(
 *         new TypeLiteral<List<Authenticator>>() {},
 *         authenticators
 *     ));
 *     return components;
 * }
 * }</pre>
 *
 * @param <T> the component type represented by the TypeLiteral
 * @param type the TypeLiteral representing the generic type for binding
 * @param component the actual component instance to be bound
 */
public record PluginComponentTypeLiteralBinding<T>(TypeLiteral<T> type, T component) {}
