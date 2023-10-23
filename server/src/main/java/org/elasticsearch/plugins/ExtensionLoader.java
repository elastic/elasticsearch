/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.util.Locale;
import java.util.ServiceLoader;
import java.util.function.Supplier;

/**
 * A utility for loading SPI extensions.
 */
public class ExtensionLoader {

    /**
     * Loads a single SPI extension.
     *
     * There should be no more than one extension found. If no service providers
     * are found, the supplied fallback is used.
     *
     * Note: A ServiceLoader is needed rather than the service class because ServiceLoaders
     * must be loaded by a module with the {@code uses} declaration. Since this
     * utility class is in server, it will not have uses (or even know about) all the
     * service classes it may load. Thus, the caller must load the ServiceLoader.
     *
     * @param loader a service loader instance to find the singleton extension in
     * @param fallback a supplier for an instance if no extensions are found
     * @return an instance of the extension
     * @param <T> the SPI extension type
     */
    public static <T> T loadSingleton(ServiceLoader<T> loader, Supplier<T> fallback) {
        var extensions = loader.stream().toList();
        if (extensions.size() > 1) {
            // It would be really nice to give the actual extension class here directly, but that would require passing it
            // in effectively twice in the call site, once to ServiceLoader, and then to this method directly as well.
            // It's annoying that ServiceLoader hangs onto the service class, but does not expose it. It does at least
            // print the service class from its toString, which is better tha nothing
            throw new IllegalStateException(String.format(Locale.ROOT, "More than one extension found for %s", loader));
        } else if (extensions.isEmpty()) {
            return fallback.get();
        }
        return extensions.get(0).get();
    }
}
