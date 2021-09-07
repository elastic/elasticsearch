/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Objects;

/**
 * An abstract provider of plugin extensions.
 *
 * This class allows customization of the creation of plugin extensions.
 *
 * If a plugin extension has a public no-arg constructor, then this mechanism is
 * not required ( but can still be used ). Otherwise, the plugin extension must
 * have a constructor with a single argument whose type is the extensible
 * plugin to which this extension applies.
 *
 * An {@link ExtensiblePlugin}, say FooPlugin, wanting to extend its
 * functionality with a FooPluginExtension, can use this facility as follows:
 *
 * 1. Declare its own subtype, say FooPluginExtensionProvider, which will be
 *    used as the service-type, when locating extensions for the FooPlugin.
 * 2. Override the {@link ExtensiblePlugin#providerType()} method to return the
 *    provider type, e.g. FooExtensionProvider.
 *
 * The extending Plugin should declare a concrete subtype for the aforementioned
 * extensible plugin's provider, say FooExtensionProvider (FEP). FEP, should
 * pass the type of actual plugin extension, FooPluginExtension, as well as the
 * type of the single constructor parameter, the extending plugin type.
 *
 */
public abstract class AbstractPluginExtensionProvider {

    private final Class<?> extensionType;
    private final Class<?> argType;  //TODO: We can make this more general, if desired

    public AbstractPluginExtensionProvider(Class<?> extensionType, Class<?> argType) {
        this.extensionType = Objects.requireNonNull(extensionType);
        this.argType = argType; // optional, may be null
    }

    /** The type of the plugin extension. */
    public Class<?> extensionType() {
        return extensionType;
    }

    /** The method type of the creator handle. */
    public MethodType methodType() {
        return MethodType.methodType(void.class, argType);
    }

    /** The <i>creator handle</i> that, when invoked, creates the plugin extension. */
    public MethodHandle creatorHandle() {
        try {
            return MethodHandles.lookup().findConstructor(extensionType(), methodType());
        } catch (Throwable t) {
            throw new AssertionError(t);  // TODO: update exception type and message
        }
    }

    // TODO: Consider adding a SelfPluginExtensionProvider. Is this useful ?
}
