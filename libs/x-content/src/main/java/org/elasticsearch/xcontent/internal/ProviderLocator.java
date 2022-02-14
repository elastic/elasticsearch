/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import org.elasticsearch.xcontent.spi.XContentProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ServiceLoader;

/**
 * A provider locator for finding the {@link XContentProvider}.
 *
 * <p> Provider implementations are located as follows:
 * <ol>
 *   <li> First the <i>provider specific</i> path is found, </li>
 *   <li> then the provider is loaded from the artifacts located at that path. </li>
 * </ol>
 *
 * <p> The provider specific path is found by searching for the <i>root providers</i> path, then resolving the specific provider
 * type against that path, <i>x-content</i> in this case. The root providers' path is an entry on the class path referring to a directory
 * in ending with "providers". Within the distribution this will be <i>lib/providers</i>, but may vary when running tests.
 *
 * <p> The artifacts that constitute the provider implementation (the impl jar and its dependencies) are explicitly named in the
 * provider-jars.txt resource file. Each name is resolved against the provider specific path to locate the actual on disk jar artifact. The
 * collection of these artifacts constitute the provider implementation.
 */
public final class ProviderLocator {

    /**
     * Returns the provider instance.
     */
    public static final XContentProvider INSTANCE = provider();

    private static XContentProvider provider() {
        try {
            PrivilegedExceptionAction<XContentProvider> pa = ProviderLocator::loadAsNonModule;
            return AccessController.doPrivileged(pa);
        } catch (PrivilegedActionException e) {
            throw new UncheckedIOException((IOException) e.getCause());
        }
    }

    private static XContentProvider loadAsNonModule() {
        ClassLoader loader = EmbeddedImplClassLoader.getInstance(ProviderLocator.class.getClassLoader(), "x-content");
        ServiceLoader<XContentProvider> sl = ServiceLoader.load(XContentProvider.class, loader);
        return sl.findFirst().orElseThrow(() -> new RuntimeException("cannot locate x-content provider"));
    }
}
