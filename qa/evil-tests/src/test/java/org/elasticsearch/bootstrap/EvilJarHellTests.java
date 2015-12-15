/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Tests for Jarhell that change very important system properties... very evil! */
@SuppressForbidden(reason  = "modifies system properties intentionally")
public class EvilJarHellTests extends ESTestCase {

    URL makeJar(Path dir, String name, Manifest manifest, String... files) throws IOException {
        Path jarpath = dir.resolve(name);
        ZipOutputStream out;
        if (manifest == null) {
            out = new JarOutputStream(Files.newOutputStream(jarpath, StandardOpenOption.CREATE));
        } else {
            out = new JarOutputStream(Files.newOutputStream(jarpath, StandardOpenOption.CREATE), manifest);
        }
        for (String file : files) {
            out.putNextEntry(new ZipEntry(file));
        }
        out.close();
        return jarpath.toUri().toURL();
    }

    public void testBootclasspathLeniency() throws Exception {
        Path dir = createTempDir();
        String previousJavaHome = System.getProperty("java.home");
        System.setProperty("java.home", dir.toString());
        URL[] jars = {makeJar(dir, "foo.jar", null, "DuplicateClass.class"), makeJar(dir, "bar.jar", null, "DuplicateClass.class")};
        try {
            JarHell.checkJarHell(jars);
        } finally {
            System.setProperty("java.home", previousJavaHome);
        }
    }

    public void testRequiredJDKVersionIsOK() throws Exception {
        Path dir = createTempDir();
        String previousJavaVersion = System.getProperty("java.specification.version");
        System.setProperty("java.specification.version", "1.7");

        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("X-Compile-Target-JDK"), "1.7");
        URL[] jars = {makeJar(dir, "foo.jar", manifest, "Foo.class")};
        try {
            JarHell.checkJarHell(jars);
        } finally {
            System.setProperty("java.specification.version", previousJavaVersion);
        }
    }

    public void testBadJDKVersionProperty() throws Exception {
        Path dir = createTempDir();
        String previousJavaVersion = System.getProperty("java.specification.version");
        System.setProperty("java.specification.version", "bogus");

        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("X-Compile-Target-JDK"), "1.7");
        URL[] jars = {makeJar(dir, "foo.jar", manifest, "Foo.class")};
        try {
            JarHell.checkJarHell(jars);
        } finally {
            System.setProperty("java.specification.version", previousJavaVersion);
        }
    }
}
