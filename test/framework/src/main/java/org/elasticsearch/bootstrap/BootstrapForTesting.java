/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import com.carrotsearch.randomizedtesting.RandomizedRunner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.filesystem.FileSystemNatives;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.network.IfConfig;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.secure_sm.SecureSM;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PrivilegedOperations;
import org.elasticsearch.test.mockito.SecureMockMaker;
import org.junit.Assert;

import java.io.Closeable;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.SocketPermission;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.Permission;
import java.security.Permissions;
import java.security.Policy;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;
import static org.elasticsearch.bootstrap.FilePermissionUtils.addDirectoryPath;

/**
 * Initializes natives and installs test security manager
 * (init'd early by base classes to ensure it happens regardless of which
 * test case happens to be first, test ordering, etc).
 * <p>
 * The idea is to mimic as much as possible what happens with ES in production
 * mode (e.g. assign permissions and install security manager the same way)
 */
public class BootstrapForTesting {

    // TODO: can we share more code with the non-test side here
    // without making things complex???

    static {
        // make sure java.io.tmpdir exists always (in case code uses it in a static initializer)
        Path javaTmpDir = PathUtils.get(
            Objects.requireNonNull(System.getProperty("java.io.tmpdir"), "please set ${java.io.tmpdir} in pom.xml")
        );
        try {
            Security.ensureDirectoryExists(javaTmpDir);
        } catch (Exception e) {
            throw new RuntimeException("unable to create test temp directory", e);
        }

        // just like bootstrap, initialize natives, then SM
        final boolean memoryLock = BootstrapSettings.MEMORY_LOCK_SETTING.get(Settings.EMPTY); // use the default bootstrap.memory_lock
                                                                                              // setting
        // some tests need the ability to disable system call filters (so they can fork other processes as part of test execution)
        final boolean systemCallFilter = Booleans.parseBoolean(System.getProperty("tests.system_call_filter", "true"));
        Elasticsearch.initializeNatives(javaTmpDir, memoryLock, systemCallFilter, true);

        // init filesystem natives
        FileSystemNatives.init();

        // initialize probes
        Elasticsearch.initializeProbes();

        // initialize sysprops
        BootstrapInfo.getSystemProperties();

        // check for jar hell
        try {
            final Logger logger = LogManager.getLogger(JarHell.class);
            JarHell.checkJarHell(logger::debug);
        } catch (Exception e) {
            throw new RuntimeException("found jar hell in test classpath", e);
        }

        // init mockito
        SecureMockMaker.init();

        // init the privileged operation
        try {
            MethodHandles.publicLookup().ensureInitialized(PrivilegedOperations.class);
        } catch (IllegalAccessException unexpected) {
            throw new AssertionError(unexpected);
        }

        // Log ifconfig output before SecurityManager is installed
        IfConfig.logIfNecessary();

        // install security manager if requested
        if (systemPropertyAsBoolean("tests.security.manager", true)) {
            try {
                // initialize paths the same exact way as bootstrap
                Permissions perms = new Permissions();
                Security.addClasspathPermissions(perms);
                // java.io.tmpdir
                FilePermissionUtils.addDirectoryPath(perms, "java.io.tmpdir", javaTmpDir, "read,readlink,write,delete", false);
                // custom test config file
                if (Strings.hasLength(System.getProperty("tests.config"))) {
                    FilePermissionUtils.addSingleFilePath(perms, PathUtils.get(System.getProperty("tests.config")), "read,readlink");
                }
                // jacoco coverage output file
                final boolean testsCoverage = Booleans.parseBoolean(System.getProperty("tests.coverage", "false"));
                if (testsCoverage) {
                    Path coverageDir = PathUtils.get(System.getProperty("tests.coverage.dir"));
                    FilePermissionUtils.addSingleFilePath(perms, coverageDir.resolve("jacoco.exec"), "read,write");
                    // in case we get fancy and use the -integration goals later:
                    FilePermissionUtils.addSingleFilePath(perms, coverageDir.resolve("jacoco-it.exec"), "read,write");
                }
                // intellij hack: intellij test runner wants setIO and will
                // screw up all test logging without it!
                if (System.getProperty("tests.gradle") == null) {
                    perms.add(new RuntimePermission("setIO"));
                }

                // add bind permissions for testing
                // ephemeral ports (note, on java 7 before update 51, this is a different permission)
                // this should really be the only one allowed for tests, otherwise they have race conditions
                perms.add(new SocketPermission("localhost:0", "listen,resolve"));
                // ... but tests are messy. like file permissions, just let them live in a fantasy for now.
                // TODO: cut over all tests to bind to ephemeral ports
                perms.add(new SocketPermission("localhost:1024-", "listen,resolve"));

                // read test-framework permissions
                Map<String, URL> codebases = getCodebases();

                final Policy testFramework = PolicyUtil.readPolicy(Elasticsearch.class.getResource("test-framework.policy"), codebases);
                final Policy runnerPolicy;
                if (System.getProperty("tests.gradle") != null) {
                    runnerPolicy = PolicyUtil.readPolicy(Elasticsearch.class.getResource("gradle.policy"), codebases);
                } else if (codebases.containsKey("junit-rt.jar")) {
                    runnerPolicy = PolicyUtil.readPolicy(Elasticsearch.class.getResource("intellij.policy"), codebases);
                } else {
                    runnerPolicy = PolicyUtil.readPolicy(Elasticsearch.class.getResource("eclipse.policy"), codebases);
                }
                // this mimicks the recursive data path permission added in Security.java
                Permissions fastPathPermissions = new Permissions();
                addDirectoryPath(fastPathPermissions, "java.io.tmpdir-fastpath", javaTmpDir, "read,readlink,write,delete", true);

                final Policy esPolicy = new ESPolicy(
                    codebases,
                    perms,
                    getPluginPermissions(),
                    true,
                    Security.toFilePermissions(fastPathPermissions)
                );
                Policy.setPolicy(new Policy() {
                    @Override
                    public boolean implies(ProtectionDomain domain, Permission permission) {
                        // implements union
                        return esPolicy.implies(domain, permission)
                            || testFramework.implies(domain, permission)
                            || runnerPolicy.implies(domain, permission);
                    }
                });
                Security.prepopulateSecurityCaller();
                Security.setSecurityManager(SecureSM.createTestSecureSM());
                Security.selfTest();

                // guarantee plugin classes are initialized first, in case they have one-time hacks.
                // this just makes unit testing more realistic
                for (URL url : Collections.list(
                    BootstrapForTesting.class.getClassLoader().getResources(PluginDescriptor.INTERNAL_DESCRIPTOR_FILENAME)
                )) {
                    Properties properties = new Properties();
                    try (InputStream stream = FileSystemUtils.openFileURLStream(url)) {
                        properties.load(stream);
                    }
                    String clazz = properties.getProperty("classname");
                    if (clazz != null) {
                        Class.forName(clazz);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("unable to install test security manager", e);
            }
        }
    }

    static Map<String, URL> getCodebases() {
        Map<String, URL> codebases = PolicyUtil.getCodebaseJarMap(JarHell.parseClassPath());
        // when testing server, the main elasticsearch code is not yet in a jar, so we need to manually add it
        addClassCodebase(codebases, "elasticsearch", "org.elasticsearch.plugins.PluginsService");
        addClassCodebase(codebases, "elasticsearch-plugin-classloader", "org.elasticsearch.plugins.loader.ExtendedPluginsClassLoader");
        addClassCodebase(codebases, "elasticsearch-nio", "org.elasticsearch.nio.ChannelFactory");
        addClassCodebase(codebases, "elasticsearch-secure-sm", "org.elasticsearch.secure_sm.SecureSM");
        addClassCodebase(codebases, "elasticsearch-rest-client", "org.elasticsearch.client.RestClient");
        addClassCodebase(codebases, "elasticsearch-core", "org.elasticsearch.core.Booleans");
        addClassCodebase(codebases, "elasticsearch-cli", "org.elasticsearch.cli.Command");
        addClassCodebase(codebases, "elasticsearch-preallocate", "org.elasticsearch.preallocate.Preallocate");
        addClassCodebase(codebases, "framework", "org.elasticsearch.test.ESTestCase");
        return codebases;
    }

    /** Add the codebase url of the given classname to the codebases map, if the class exists. */
    private static void addClassCodebase(Map<String, URL> codebases, String name, String classname) {
        try {
            if (codebases.containsKey(name)) {
                return; // the codebase already exists, from the classpath
            }
            Class<?> clazz = BootstrapForTesting.class.getClassLoader().loadClass(classname);
            URL location = clazz.getProtectionDomain().getCodeSource().getLocation();
            if (location.toString().endsWith(".jar") == false) {
                if (codebases.put(name, location) != null) {
                    throw new IllegalStateException("Already added " + name + " codebase for testing");
                }
            }
        } catch (ClassNotFoundException e) {
            // no class, fall through to not add. this can happen for any tests that do not include
            // the given class. eg only core tests include plugin-classloader
        }
    }

    /**
     * we don't know which codesources belong to which plugin, so just remove the permission from key codebases
     * like core, test-framework, etc. this way tests fail if accesscontroller blocks are missing.
     */
    @SuppressForbidden(reason = "accesses fully qualified URLs to configure security")
    static Map<String, Policy> getPluginPermissions() throws Exception {
        List<URL> pluginPolicies = Collections.list(
            BootstrapForTesting.class.getClassLoader().getResources(PluginDescriptor.ES_PLUGIN_POLICY)
        );
        if (pluginPolicies.isEmpty()) {
            return Collections.emptyMap();
        }

        // compute classpath minus obvious places, all other jars will get the permission.
        Set<URL> codebases = new HashSet<>(parseClassPathWithSymlinks());
        Set<URL> excluded = new HashSet<>(
            Arrays.asList(
                // es core
                Elasticsearch.class.getProtectionDomain().getCodeSource().getLocation(),
                // es test framework
                BootstrapForTesting.class.getProtectionDomain().getCodeSource().getLocation(),
                // lucene test framework
                LuceneTestCase.class.getProtectionDomain().getCodeSource().getLocation(),
                // randomized runner
                RandomizedRunner.class.getProtectionDomain().getCodeSource().getLocation(),
                // junit library
                Assert.class.getProtectionDomain().getCodeSource().getLocation()
            )
        );
        codebases.removeAll(excluded);
        final Map<String, URL> codebasesMap = PolicyUtil.getCodebaseJarMap(codebases);

        // parse each policy file, with codebase substitution from the classpath
        final List<Policy> policies = new ArrayList<>(pluginPolicies.size());
        for (URL policyFile : pluginPolicies) {
            Map<String, URL> policyCodebases = codebasesMap;

            // if the codebases file is inside a jar, then we don't need to load it since the jar will
            // have already been read from the classpath
            if (policyFile.toString().contains(".jar!") == false) {
                Path policyPath = PathUtils.get(policyFile.toURI());
                Path codebasesPath = policyPath.getParent().resolve("plugin-security.codebases");

                if (Files.exists(codebasesPath)) {
                    // load codebase to class map used for tests
                    policyCodebases = new HashMap<>(codebasesMap);
                    Map<String, String> codebasesProps = parsePropertiesFile(codebasesPath);
                    for (var entry : codebasesProps.entrySet()) {
                        addClassCodebase(policyCodebases, entry.getKey(), entry.getValue());
                    }
                }
            }

            policies.add(PolicyUtil.readPolicy(policyFile, policyCodebases));
        }

        // consult each policy file for those codebases
        Map<String, Policy> map = new HashMap<>();
        for (URL url : codebases) {
            map.put(url.getFile(), new Policy() {
                @Override
                public boolean implies(ProtectionDomain domain, Permission permission) {
                    // implements union
                    for (Policy p : policies) {
                        if (p.implies(domain, permission)) {
                            return true;
                        }
                    }
                    return false;
                }
            });
        }
        return Collections.unmodifiableMap(map);
    }

    static Map<String, String> parsePropertiesFile(Path propertiesFile) throws Exception {
        Properties props = new Properties();
        try (InputStream is = Files.newInputStream(propertiesFile)) {
            props.load(is);
        }
        return props.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
    }

    /**
     * return parsed classpath, but with symlinks resolved to destination files for matching
     * this is for matching the toRealPath() in the code where we have a proper plugin structure
     */
    @SuppressForbidden(reason = "does evil stuff with paths and urls because devs and jenkins do evil stuff with paths and urls")
    static Set<URL> parseClassPathWithSymlinks() throws Exception {
        Set<URL> raw = JarHell.parseClassPath();
        Set<URL> cooked = Sets.newHashSetWithExpectedSize(raw.size());
        for (URL url : raw) {
            Path path = PathUtils.get(url.toURI());
            if (Files.exists(path)) {
                boolean added = cooked.add(path.toRealPath().toUri().toURL());
                if (added == false) {
                    throw new IllegalStateException("Duplicate in classpath after resolving symlinks: " + url);
                }
            }
        }
        return raw;
    }

    // does nothing, just easy way to make sure the class is loaded.
    public static void ensureInitialized() {}

    /**
     * Temporarily dsiables security manager for a test.
     *
     * <p> This method is only callable by {@link org.elasticsearch.test.ESTestCase}.
     *
     * @return A closeable object which restores the test security manager
     */
    @SuppressWarnings("removal")
    public static Closeable disableTestSecurityManager() {
        var caller = Thread.currentThread().getStackTrace()[2];
        if (ESTestCase.class.getName().equals(caller.getClassName()) == false) {
            throw new SecurityException("Cannot disable test SecurityManager directly. Use @NoSecurityManager to disable on a test suite");
        }
        final var sm = System.getSecurityManager();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            Security.setSecurityManager(null);
            return null;
        });
        return () -> AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            Security.setSecurityManager(sm);
            return null;
        });
    }
}
