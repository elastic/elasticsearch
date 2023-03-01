/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.plugins.Plugin;

/**
 *<p>
 * Describes the relationship between an interface and a provider to the implementation for {@link Plugin} components. All {@link Plugin}'s
 * must implement the plugin interface which requires returning a collection of components via createComponents(...). Those components are
 * then made available for dependency injection while setting up the {@link Node}. The common convention is for a plugin is to create
 * a concrete implementation and return that instance from createComponents(...). However, that does not work if a plugin
 * wants to contribute a different implementation of an interface based on some condition defined within the plugin. This class exists
 * so that a plugin can define the interface and then conditionally define the implementation which is wired in for dependency injection.
 *</p>
 *
 *     Usage:
 * <pre>{@code

//TODO...
}
 * </pre>
 *
 *
 *<p>
 * Note - usage of the class does not change any class loader isolation strategies nor visibility of the provided classes.
 * </p>
 * @param <C> The interface
 * @param <I> A provider to the instance of the interface
 */
public record PluginComponentInterface<C, I extends C> (Class<? extends C> clazz, Provider<I> provider) {}
