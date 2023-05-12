/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;

import org.elasticsearch.plugin.Extensible;
import org.elasticsearch.plugin.NamedComponent;
import org.elasticsearch.plugin.scanner.test_classes.ExtensibleClass;
import org.elasticsearch.plugin.scanner.test_classes.ExtensibleInterface;

import java.io.IOException;
import java.nio.file.Path;

public final class StableApiJarMocks {
    private StableApiJarMocks() {}

    public static Path createExtensibleApiJar(Path jar) throws IOException {
        jar = jar.resolve("plugin-extensible-api.jar");
        DynamicType.Unloaded<ExtensibleInterface> extensible = new ByteBuddy().decorate(ExtensibleInterface.class).make();

        DynamicType.Unloaded<ExtensibleClass> extensibleClass = new ByteBuddy().decorate(ExtensibleClass.class).make();

        extensible.toJar(jar.toFile());
        extensibleClass.inject(jar.toFile());
        return jar;
    }

    public static Path createPluginApiJar(Path jar) throws IOException {
        jar = jar.resolve("plugin-api.jar");

        DynamicType.Unloaded<Extensible> extensible = new ByteBuddy().decorate(Extensible.class).make();
        extensible.toJar(jar.toFile());
        DynamicType.Unloaded<NamedComponent> namedComponent = new ByteBuddy().decorate(NamedComponent.class).make();
        extensible.toJar(jar.toFile());
        namedComponent.inject(jar.toFile());
        return jar;
    }
}
