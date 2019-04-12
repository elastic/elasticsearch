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
import org.elasticsearch.common.io.PathUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
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
        checkJarHell(System.out::println);
        System.out.println("no jar hell found");
    }

    /**
     * Checks the current classpath for duplicate classes
     * @param output A {@link String} {@link Consumer} to which debug output will be sent
     * @throws IllegalStateException if jar hell was found
     */
    public static void checkJarHell(Consumer<String> output) throws IOException, URISyntaxException {
        ClassLoader loader = JarHell.class.getClassLoader();
        output.accept("java.class.path: " + System.getProperty("java.class.path"));
        output.accept("sun.boot.class.path: " + System.getProperty("sun.boot.class.path"));
        if (loader instanceof URLClassLoader) {
            output.accept("classloader urls: " + Arrays.toString(((URLClassLoader)loader).getURLs()));
        }
        checkJarHell(parseClassPath(), output);
    }

    /**
     * Parses the classpath into an array of URLs
     * @return array of URLs
     * @throws IllegalStateException if the classpath contains empty elements
     */
    public static Set<URL> parseClassPath()  {
        return parseClassPath(System.getProperty("java.class.path"));
    }

    /**
     * Parses the classpath into a set of URLs. For testing.
     * @param classPath classpath to parse (typically the system property {@code java.class.path})
     * @return array of URLs
     * @throws IllegalStateException if the classpath contains empty elements
     */
    @SuppressForbidden(reason = "resolves against CWD because that is how classpaths work")
    static Set<URL> parseClassPath(String classPath) {
        String pathSeparator = System.getProperty("path.separator");
        String fileSeparator = System.getProperty("file.separator");
        String elements[] = classPath.split(pathSeparator);
        Set<URL> urlElements = new LinkedHashSet<>(); // order is already lost, but some filesystems have it
        for (String element : elements) {
            // Technically empty classpath element behaves like CWD.
            // So below is the "correct" code, however in practice with ES, this is usually just a misconfiguration,
            // from old shell scripts left behind or something:
            //   if (element.isEmpty()) {
            //      element = System.getProperty("user.dir");
            //   }
            // Instead we just throw an exception, and keep it clean.
            if (element.isEmpty()) {
                throw new IllegalStateException("Classpath should not contain empty elements! (outdated shell script from a previous" +
                    " version?) classpath='" + classPath + "'");
            }
            // we should be able to just Paths.get() each element, but unfortunately this is not the
            // whole story on how classpath parsing works: if you want to know, start at sun.misc.Launcher,
            // be sure to stop before you tear out your eyes. we just handle the "alternative" filename
            // specification which java seems to allow, explicitly, right here...
            if (element.startsWith("/") && "\\".equals(fileSeparator)) {
                // "correct" the entry to become a normal entry
                // change to correct file separators
                element = element.replace("/", "\\");
                // if there is a drive letter, nuke the leading separator
                if (element.length() >= 3 && element.charAt(2) == ':') {
                    element = element.substring(1);
                }
            }
            // now just parse as ordinary file
            try {
                URL url = PathUtils.get(element).toUri().toURL();
                if (urlElements.add(url) == false) {
                    throw new IllegalStateException("jar hell!" + System.lineSeparator() +
                        "duplicate jar [" + element + "] on classpath: " + classPath);
                }
            } catch (MalformedURLException e) {
                // should not happen, as we use the filesystem API
                throw new RuntimeException(e);
            }
        }
        return Collections.unmodifiableSet(urlElements);
    }

    /**
     * Checks the set of URLs for duplicate classes
     * @param urls A set of URLs from the classpath to be checked for conflicting jars
     * @param output A {@link String} {@link Consumer} to which debug output will be sent
     * @throws IllegalStateException if jar hell was found
     */
    @SuppressForbidden(reason = "needs JarFile for speed, just reading entries")
    public static void checkJarHell(Set<URL> urls, Consumer<String> output) throws URISyntaxException, IOException {
        // we don't try to be sneaky and use deprecated/internal/not portable stuff
        // like sun.boot.class.path, and with jigsaw we don't yet have a way to get
        // a "list" at all. So just exclude any elements underneath the java home
        String javaHome = System.getProperty("java.home");
        output.accept("java.home: " + javaHome);
        final Map<String,Path> clazzes = new HashMap<>(32768);
        Set<Path> seenJars = new HashSet<>();
        for (final URL url : urls) {
            final Path path = PathUtils.get(url.toURI());
            // exclude system resources
            if (path.startsWith(javaHome)) {
                output.accept("excluding system resource: " + path);
                continue;
            }
            if (path.toString().endsWith(".jar")) {
                if (!seenJars.add(path)) {
                    throw new IllegalStateException("jar hell!" + System.lineSeparator() +
                                                    "duplicate jar on classpath: " + path);
                }
                output.accept("examining jar: " + path);
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
                output.accept("examining directory: " + path);
                // case for tests: where we have class files in the classpath
                final Path root = PathUtils.get(url.toURI());
                final String sep = root.getFileSystem().getSeparator();

                // don't try and walk class or resource directories that don't exist
                // gradle will add these to the classpath even if they never get created
                if (Files.exists(root)) {
                    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            String entry = root.relativize(file).toString();
                            if (entry.endsWith(".class")) {
                                // normalize with the os separator, remove '.class'
                                entry = entry.replace(sep, ".").substring(0, entry.length() - ".class".length());
                                checkClass(clazzes, entry, path);
                            }
                            return super.visitFile(file, attrs);
                        }
                    });
                }
            }
        }
    }

    /** inspect manifest for sure incompatibilities */
    private static void checkManifest(Manifest manifest, Path jar) {
        // give a nice error if jar requires a newer java version
        String targetVersion = manifest.getMainAttributes().getValue("X-Compile-Target-JDK");
        if (targetVersion != null) {
            checkVersionFormat(targetVersion);
            checkJavaVersion(jar.toString(), targetVersion);
        }
    }

    public static void checkVersionFormat(String targetVersion) {
        if (!JavaVersion.isValid(targetVersion)) {
            throw new IllegalStateException(
                    String.format(
                            Locale.ROOT,
                            "version string must be a sequence of nonnegative decimal integers separated by \".\"'s and may have " +
                                "leading zeros but was %s",
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

    private static void checkClass(Map<String, Path> clazzes, String clazz, Path jarpath) {
        if (clazz.equals("module-info") || clazz.endsWith(".module-info")) {
            // Ignore jigsaw module descriptions
            return;
        }
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
                throw new IllegalStateException("jar hell!" + System.lineSeparator() +
                        "class: " + clazz + System.lineSeparator() +
                        "jar1: " + previous + System.lineSeparator() +
                        "jar2: " + jarpath);
            }
        }
    }
}
