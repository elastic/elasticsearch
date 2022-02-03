/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.spi;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.cbor.CborXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ServiceLoader;
import java.util.Set;

public interface XContentProvider {

    interface FormatProvider<T> {
        XContentBuilder getContentBuilder() throws IOException;

        T XContent();
    }

    FormatProvider<CborXContent> getCborXContent();

    FormatProvider<JsonXContent> getJsonXContent();

    FormatProvider<SmileXContent> getSmileXContent();

    FormatProvider<YamlXContent> getYamlXContent();

    XContentParserConfiguration empty();

    static XContentProvider provider() {
        return Holder.INSTANCE;
    }

    class Holder {
        static final XContentProvider INSTANCE = provider();

        private static XContentProvider provider() {
            try {
                URI uri = asDirectory(XContentProvider.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                Path libPath = Path.of(uri).resolve("xcontent_provider");
                if (Files.notExists(libPath) || Files.isDirectory(libPath) == false) {
                    throw new UncheckedIOException(new FileNotFoundException(libPath.toString()));
                }

                if (XContentProvider.class.getModule().isNamed()) {
                    return loadAsModule(libPath);
                } else {
                    return loadAsNonModule(libPath);
                }
            } catch (URISyntaxException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static final String PROVIDER_MODULE_NAME = "org.elasticsearch.xcontent.provider";

        private static XContentProvider loadAsModule(Path libPath) {
            ModuleLayer parent = ModuleLayer.boot();
            Configuration cf = parent.configuration().resolve(ModuleFinder.of(), ModuleFinder.of(libPath), Set.of(PROVIDER_MODULE_NAME));
            ModuleLayer layer = parent.defineModulesWithOneLoader(cf, XContentProvider.class.getClassLoader());
            Module m = XContentProvider.class.getModule();
            m.addExports("org.elasticsearch.xcontent.spi", layer.findModule(PROVIDER_MODULE_NAME).orElseThrow());
            m.addExports("org.elasticsearch.xcontent.support.filtering", layer.findModule(PROVIDER_MODULE_NAME).orElseThrow());
            ServiceLoader<XContentProvider> sl = ServiceLoader.load(layer, XContentProvider.class);
            return sl.findFirst().orElseThrow();
        }

        private static XContentProvider loadAsNonModule(Path libPath) {
            try {
                URL[] urls = Files.walk(libPath).filter(path -> path.toString().endsWith(".jar")).map(Holder::toURL).toArray(URL[]::new);
                URLClassLoader loader = URLClassLoader.newInstance(urls, XContentProvider.class.getClassLoader());
                ServiceLoader<XContentProvider> sl = ServiceLoader.load(XContentProvider.class, loader);
                return sl.findFirst().orElseThrow();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static URL toURL(Path path) {
            try {
                return path.toUri().toURL();
            } catch (MalformedURLException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static URI asDirectory(URI uri) throws IOException {
            String uriString = uri.toString();
            int idx = uriString.lastIndexOf("/");
            if (idx == -1) {
                throw new IOException("malformed uri: " + uri);
            }
            return URI.create(uriString.substring(0, idx + 1));
        }
    }
}
