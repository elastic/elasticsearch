/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.Assert.fail;

public class TestClasspathUtils {

    public static void setupJarJdkClasspath(File projectRoot) {
        try {
            URL originLocation = TestClasspathUtils.class.getClassLoader()
                .loadClass("org.elasticsearch.bootstrap.JdkJarHellCheck")
                .getProtectionDomain()
                .getCodeSource()
                .getLocation();
            File targetFile = new File(
                    projectRoot,
                    "sample_jars/build/testrepo/org/elasticsearch/elasticsearch-core/current/elasticsearch-core-current.jar"
            );

            targetFile.getParentFile().mkdirs();
            File sourceFile = new File(originLocation.toURI());
            if(sourceFile.isDirectory()) {
                if(targetFile.isDirectory()) {
                    Files.delete(targetFile.toPath());
                }
                Manifest manifest = new Manifest();
                manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
                JarOutputStream target = new JarOutputStream(new FileOutputStream(targetFile), manifest);
                for (File nestedFile : sourceFile.listFiles()) {
                    add("", nestedFile, target);
                }
                target.close();
            } else {
                targetFile.getParentFile().mkdirs();
                Path originalPath = Paths.get(originLocation.toURI());
                Files.copy(originalPath, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (ClassNotFoundException | URISyntaxException | IOException e) {
            e.printStackTrace();
            fail("Cannot setup jdk jar hell classpath");
        }
    }

    private static void add(String parents, File source, JarOutputStream target) throws IOException {
        BufferedInputStream in = null;
        try {
            String name = (parents + source.getName()).replace("\\", "/");
            if (source.isDirectory()) {
                if (!name.isEmpty()) {
                    if (!name.endsWith("/"))
                        name += "/";
                    JarEntry entry = new JarEntry(name);
                    entry.setTime(source.lastModified());
                    target.putNextEntry(entry);
                    target.closeEntry();
                }
                for (File nestedFile : source.listFiles()) {
                    add(name, nestedFile, target);
                }
                return;
            }

            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            in = new BufferedInputStream(new FileInputStream(source));
            byte[] buffer = new byte[1024];
            while (true) {
                int count = in.read(buffer);
                if (count == -1) {
                    break;
                }
                target.write(buffer, 0, count);
            }
            target.closeEntry();
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

}
