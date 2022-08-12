/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.module;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.exportsOf;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.opensOf;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;

public class BasicServerModuleTests extends ESTestCase {

    private static final String SERVER_MODULE_NAME = "org.elasticsearch.server";

    public void testQualifiedExports() {
        var md = getServerDescriptor();

        // The package containing the RestInterceptor type, org.elasticsearch.plugins.interceptor,
        // should only be exported to security.
        assertThat(md.exports(), hasItem(exportsOf("org.elasticsearch.plugins.interceptor", Set.of("org.elasticsearch.security"))));

        // additional qualified export constraint go here
    }

    // Ensures that there are no unqualified opens - too dangerous
    public void testEnsureNoUnqualifiedOpens() {
        var md = getServerDescriptor();
        assertThat(md.opens().stream().filter(not(ModuleDescriptor.Opens::isQualified)).collect(Collectors.toSet()), empty());
    }

    public void testQualifiedOpens() {
        var md = getServerDescriptor();

        // We tolerate introspection by log4j, for Plugins, e.g. ECSJsonLayout
        assertThat(md.opens(), hasItem(opensOf("org.elasticsearch.common.logging", Set.of("org.apache.logging.log4j.core"))));

        // additional qualified opens constraint go here
    }

    private static ModuleDescriptor getServerDescriptor() {
        var finder = ModuleFinder.of(urlsToPaths(JarHell.parseClassPath()));
        var omref = finder.find(SERVER_MODULE_NAME);
        assertThat(omref, isPresent());
        return omref.get().descriptor();
    }

    @SuppressForbidden(reason = "I need to convert URL's to Paths")
    private static Path[] urlsToPaths(Set<URL> urls) {
        return urls.stream().map(BasicServerModuleTests::uncheckedToURI).map(PathUtils::get).toArray(Path[]::new);
    }

    private static URI uncheckedToURI(URL url) {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new UncheckedIOException(new IOException(e));
        }
    }
}
