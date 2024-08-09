/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.injector.step;

/**
 * Causes the proxy for the given list to become usable.
 * This is mandatory if the list is to be injected using {@link org.elasticsearch.nalbind.api.Actual}.
 * It is an error for there to be any subsequent {@link InstanceSupplyingStep}s for
 * the {@link #elementType}, because it's too late to add them to the list.
 */
public record ListProxyResolveStep(
    Class<?> elementType
) implements InjectionStep { }
