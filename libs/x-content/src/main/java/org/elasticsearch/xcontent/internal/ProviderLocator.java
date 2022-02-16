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
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * A provider locator for finding the {@link XContentProvider}.
 */
public final class ProviderLocator {

    /**
     * Returns the provider instance.
     */
    public static final XContentProvider INSTANCE = provider();

    private static XContentProvider provider() {
        try {
            PrivilegedExceptionAction<XContentProvider> pa = ProviderLocator::load;
            return AccessController.doPrivileged(pa);
        } catch (PrivilegedActionException e) {
            throw new UncheckedIOException((IOException) e.getCause());
        }
    }

    private static final String PROVIDER_NAME = "x-content";

    private static XContentProvider load() {
        if (XContentProvider.class.getModule().isNamed()) {
            return loadAsModule();
        } else {
            return loadAsNonModule();
        }
    }

    private static XContentProvider loadAsNonModule() {
        ClassLoader loader = EmbeddedImplClassLoader.getInstance(ProviderLocator.class.getClassLoader(), PROVIDER_NAME);
        ServiceLoader<XContentProvider> sl = ServiceLoader.load(XContentProvider.class, loader);
        return sl.findFirst().orElseThrow(() -> new RuntimeException("cannot locate x-content provider"));
    }

    private static final String PROVIDER_MODULE_NAME = "org.elasticsearch.xcontent.impl";

    private static XContentProvider loadAsModule() {
        try {
            Map<String, CodeSource> providerPrefixes = EmbeddedImplUtils.getProviderPrefixes(
                ClassLoader.getSystemClassLoader(),
                PROVIDER_NAME
            );
            Map.Entry<String, CodeSource> entry = providerPrefixes.entrySet().iterator().next();
            URI embeddedJarURI = entry.getValue().getLocation().toURI();
            URI rootURI = toRootURI(embeddedJarURI);
            ModuleFinder moduleFinder = embeddedModuleFinder(rootURI, providerPrefixes);
            assert moduleFinder.find("org.elasticsearch.xcontent.impl").isPresent();

            ModuleLayer parent = ModuleLayer.boot();
            Configuration cf = parent.configuration().resolve(ModuleFinder.of(), moduleFinder, Set.of(PROVIDER_MODULE_NAME));
            ModuleLayer layer = parent.defineModulesWithOneLoader(cf, XContentProvider.class.getClassLoader());
            // Module m = XContentProvider.class.getModule();
            // m.addExports("org.elasticsearch.xcontent.spi", layer.findModule(PROVIDER_MODULE_NAME).orElseThrow());
            // m.addExports("org.elasticsearch.xcontent.support.filtering", layer.findModule(PROVIDER_MODULE_NAME).orElseThrow());
            ServiceLoader<XContentProvider> sl = ServiceLoader.load(layer, XContentProvider.class);
            return sl.findFirst().orElseThrow();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static ModuleFinder embeddedModuleFinder(URI uri, Map<String, CodeSource> prefixToCodeBase) throws IOException {
        Path classes;
        if (uri.getScheme().equals("file")) {
            classes = Path.of(uri);
        } else if (uri.getScheme().equals("jar")) {
            var fileSystem = FileSystems.newFileSystem(uri, Map.of(), ClassLoader.getSystemClassLoader());
            classes = fileSystem.getPath("/");
        } else {
            throw new UncheckedIOException(new IOException("unknown scheme:" + uri.getScheme()));
        }
        var moduleFinder = ModuleFinder.of(prefixToCodeBase.keySet().stream().map(p -> classes.resolve(p)).toArray(Path[]::new));
        return moduleFinder;
    }

    /**
     * Returns the root uri path for the given embedded jar URI.
     *
     * Depending on whether running from a jar (distroution), or an exploded archive (testing), URIs will have one of two schemed, "file",
     * or "jar:file". For example:
     *  distro- jar:file:/xxx/distro/lib/elasticsearch-x-content-8.2.0-SNAPSHOT.jar!/IMPL-JARS/x-content/xlib-2.10.4.jar
     *  test  - file:/x/git/es_modules/libs/x-content/build/generated-resources/impl/IMPL-JARS/x-content/xlib-2.10.4.jar
     */
    static URI toRootURI(URI uri) {
        if (uri.getScheme().equals("jar")) {
            String s = uri.toString();
            return URI.create(s.substring(0, s.lastIndexOf("!/")));
        } else {
            return URI.create(getParent(getParent(getParent(uri.toString()))));
        }
    }

    static String getParent(String uriString) {
        int index = uriString.lastIndexOf('/');
        if (index > 0) {
            return uriString.substring(0, index);
        }
        return "/";
    }
}
