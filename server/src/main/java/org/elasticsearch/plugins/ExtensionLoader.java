/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.core.Strings;

import java.util.Optional;
import java.util.ServiceLoader;

/**
 * A utility for loading SPI extensions.
 */
public class ExtensionLoader {

    /**
     * Loads a single SPI extension.
     *
     * There should be no more than one extension found.
     *
     * Note: A ServiceLoader is needed rather than the service class because ServiceLoaders
     * must be loaded by a module with the {@code uses} declaration. Since this
     * utility class is in server, it will not have uses (or even know about) all the
     * service classes it may load. Thus, the caller must load the ServiceLoader.
     *
     * @param loader a service loader instance to find the singleton extension in
     * @return an instance of the extension
     * @param <T> the SPI extension type
     */
    public static <T> Optional<T> loadSingleton(ServiceLoader<T> loader) {
        var extensions = loader.iterator();
        if (extensions.hasNext() == false) {
            return Optional.empty();
        }
        var ext = extensions.next();
        if (extensions.hasNext()) {
            // It would be really nice to give the actual extension class here directly, but that would require passing it
            // in effectively twice in the call site, once to ServiceLoader, and then to this method directly as well.
            // It's annoying that ServiceLoader hangs onto the service class, but does not expose it. It does at least
            // print the service class from its toString, which is better tha nothing
            throw new IllegalStateException(Strings.format("More than one extension found for %s", loader));
        }
        return Optional.of(ext);
    }
}
