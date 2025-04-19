/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.IfConfig;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.test.PrivilegedOperations;
import org.elasticsearch.test.mockito.SecureMockMaker;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

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
            Files.createDirectories(javaTmpDir);
        } catch (IOException e) {
            throw new RuntimeException("unable to create test temp directory", e);
        }

        // just like bootstrap, initialize natives, then SM
        final boolean memoryLock = BootstrapSettings.MEMORY_LOCK_SETTING.get(Settings.EMPTY); // use the default bootstrap.memory_lock
                                                                                              // setting
        // some tests need the ability to disable system call filters (so they can fork other processes as part of test execution)
        final boolean systemCallFilter = Booleans.parseBoolean(System.getProperty("tests.system_call_filter", "true"));
        Elasticsearch.initializeNatives(javaTmpDir, memoryLock, systemCallFilter, true);

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
        addClassCodebase(codebases, "elasticsearch-simdvec", "org.elasticsearch.simdvec.VectorScorerFactory");
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

    // does nothing, just easy way to make sure the class is loaded.
    public static void ensureInitialized() {}
}
