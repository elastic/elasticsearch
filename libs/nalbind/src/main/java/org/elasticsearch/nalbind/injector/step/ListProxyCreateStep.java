/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.injector.step;

/**
 * Creates a proxy for lists of the given type.
 * Subsequent steps that inject such a list will receive the proxy
 * unless it is resolved first via a {@link ListProxyResolveStep}.
 * The proxy will throw exceptions if it is used before it is resolved.
 */
public record ListProxyCreateStep(
    Class<?> elementType
) implements InjectionStep { }
