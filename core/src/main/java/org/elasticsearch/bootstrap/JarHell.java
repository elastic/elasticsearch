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

import org.elasticsearch.Version;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Simple check for duplicate class files across the classpath.
 * <p>
 * This class checks for incompatibilities in the following ways:
 * <ul>
 *   <li>Checks that class files are not duplicated across jars.</li>
 *   <li>Checks any {@code X-Compile-Target-JDK} value in the jar
 *       manifest is compatible with current JRE</li>
 *   <li>Checks any {@code X-Compile-Elasticsearch-Version} value in
 *       the jar manifest is compatible with the current ES</li>
 * </ul>
 */
public class JarHell {

    /** no instantiation */
    private JarHell() {}

    /** Simple driver class, can be used eg. from builds. Returns non-zero on jar-hell */
    @SuppressForbidden(reason = "command line tool")
    public static void main(String args[]) throws Exception {
        System.out.println("checking for jar hell...");
        checkJarHell();
        System.out.println("no jar hell found");
    }

    /**
     * Checks the current classpath for duplicate classes
     * @throws IllegalStateException if jar hell was found
     */
    public static void checkJarHell() throws Exception {
        ClassLoader loader = JarHell.class.getClassLoader();
        ESLogger logger = Loggers.getLogger(JarHell.class);
        if (logger.isDebugEnabled()) {
            logger.debug("java.class.path: {}", System.getProperty("java.class.path"));
            logger.debug("sun.boot.class.path: {}", System.getProperty("sun.boot.class.path"));
            if (loader instanceof URLClassLoader ) {
                logger.debug("classloader urls: {}", Arrays.toString(((URLClassLoader)loader).getURLs()));
             }
        }
        checkJarHell(parseClassPath());
    }
    
    /**
     * Parses the classpath into a set of URLs
     */
    @SuppressForbidden(reason = "resolves against CWD because that is how classpaths work")
    public static URL[] parseClassPath()  {
        String elements[] = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        URL urlElements[] = new URL[elements.length];
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            // empty classpath element behaves like CWD.
            if (element.isEmpty()) {
                element = System.getProperty("user.dir");
            }
            try {
                urlElements[i] = PathUtils.get(element).toUri().toURL();
            } catch (MalformedURLException e) {
                // should not happen, as we use the filesystem API
                throw new RuntimeException(e);
            }
        }
        return urlElements;
    }

    /**
     * Checks the set of URLs for duplicate classes
     * @throws IllegalStateException if jar hell was found
     */
    @SuppressForbidden(reason = "needs JarFile for speed, just reading entries")
    public static void checkJarHell(URL urls[]) throws Exception {
        ESLogger logger = Loggers.getLogger(JarHell.class);
        // we don't try to be sneaky and use deprecated/internal/not portable stuff
        // like sun.boot.class.path, and with jigsaw we don't yet have a way to get
        // a "list" at all. So just exclude any elements underneath the java home
        String javaHome = System.getProperty("java.home");
        logger.debug("java.home: {}", javaHome);
        final Map<String,Path> clazzes = new HashMap<>(32768);
        Set<Path> seenJars = new HashSet<>();
        for (final URL url : urls) {
            final Path path = PathUtils.get(url.toURI());
            // exclude system resources
            if (path.startsWith(javaHome)) {
                logger.debug("excluding system resource: {}", path);
                continue;
            }
            if (path.toString().endsWith(".jar")) {
                if (!seenJars.add(path)) {
                    logger.debug("excluding duplicate classpath element: {}", path);
                    continue; // we can't fail because of sheistiness with joda-time
                }
                logger.debug("examining jar: {}", path);
                try (JarFile file = new JarFile(path.toString())) {
                    Manifest manifest = file.getManifest();
                    if (manifest != null) {
                        checkManifest(manifest, path);
                    }
                    // inspect entries
                    Enumeration<JarEntry> elements = file.entries();
                    while (elements.hasMoreElements()) {
                        String entry = elements.nextElement().getName();
                        if (entry.endsWith(".class")) {
                            // for jar format, the separator is defined as /
                            entry = entry.replace('/', '.').substring(0, entry.length() - 6);
                            checkClass(clazzes, entry, path);
                        }
                    }
                }
            } else {
                logger.debug("examining directory: {}", path);
                // case for tests: where we have class files in the classpath
                final Path root = PathUtils.get(url.toURI());
                final String sep = root.getFileSystem().getSeparator();
                Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        String entry = root.relativize(file).toString();
                        if (entry.endsWith(".class")) {
                            // normalize with the os separator
                            entry = entry.replace(sep, ".").substring(0,  entry.length() - 6);
                            checkClass(clazzes, entry, path);
                        }
                        return super.visitFile(file, attrs);
                    }
                });
            }
        }
    }

    /** inspect manifest for sure incompatibilities */
    static void checkManifest(Manifest manifest, Path jar) {
        // give a nice error if jar requires a newer java version
        String targetVersion = manifest.getMainAttributes().getValue("X-Compile-Target-JDK");
        if (targetVersion != null) {
            checkVersionFormat(targetVersion);
            checkJavaVersion(jar.toString(), targetVersion);
        }

        // give a nice error if jar is compiled against different es version
        String systemESVersion = Version.CURRENT.toString();
        String targetESVersion = manifest.getMainAttributes().getValue("X-Compile-Elasticsearch-Version");
        if (targetESVersion != null && targetESVersion.equals(systemESVersion) == false) {
            throw new IllegalStateException(jar + " requires Elasticsearch " + targetESVersion
                    + ", your system: " + systemESVersion);
        }
    }

    public static void checkVersionFormat(String targetVersion) {
        if (!JavaVersion.isValid(targetVersion)) {
            throw new IllegalStateException(
                    String.format(
                            Locale.ROOT,
                            "version string must be a sequence of nonnegative decimal integers separated by \".\"'s and may have leading zeros but was %s",
                            targetVersion
                    )
            );
        }
    }

    /**
     * Checks that the java specification version {@code targetVersion}
     * required by {@code resource} is compatible with the current installation.
     */
    public static void checkJavaVersion(String resource, String targetVersion) {
        JavaVersion version = JavaVersion.parse(targetVersion);
        if (JavaVersion.current().compareTo(version) < 0) {
            throw new IllegalStateException(
                    String.format(
                            Locale.ROOT,
                            "%s requires Java %s:, your system: %s",
                            resource,
                            targetVersion,
                            JavaVersion.current().toString()
                    )
            );
        }
    }

    static void checkClass(Map<String,Path> clazzes, String clazz, Path jarpath) {
        Path previous = clazzes.put(clazz, jarpath);
        if (previous != null) {
            if (previous.equals(jarpath)) {
                if (clazz.startsWith("org.apache.xmlbeans")) {
                    return; // https://issues.apache.org/jira/browse/XMLBEANS-499
                }
                // throw a better exception in this ridiculous case.
                // unfortunately the zip file format allows this buggy possibility
                // UweSays: It can, but should be considered as bug :-)
                throw new IllegalStateException("jar hell!" + System.lineSeparator() +
                        "class: " + clazz + System.lineSeparator() +
                        "exists multiple times in jar: " + jarpath + " !!!!!!!!!");
            } else {
                if (clazz.startsWith("org.apache.log4j")) {
                    return; // go figure, jar hell for what should be System.out.println...
                }
                if (clazz.equals("org.joda.time.base.BaseDateTime")) {
                    return; // apparently this is intentional... clean this up
                }
                throw new IllegalStateException("jar hell!" + System.lineSeparator() +
                        "class: " + clazz + System.lineSeparator() +
                        "jar1: " + previous + System.lineSeparator() +
                        "jar2: " + jarpath);
            }
        }
    }
}
