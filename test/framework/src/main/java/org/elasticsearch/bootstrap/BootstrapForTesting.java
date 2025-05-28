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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.List;
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

    private static final ConstructingObjectParser<TestBuildInfoLocation, Void> TEST_BUILD_INFO_LOCATION_PARSER =
        new ConstructingObjectParser<>("test_build_info_location", values -> new TestBuildInfoLocation((String)values[0], (String)values[1]));
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TestBuildInfo, Void> TEST_BUILD_INFO_PARSER =
        new ConstructingObjectParser<>("test_build_info", values -> new TestBuildInfo((String)values[0], (List<TestBuildInfoLocation>)values[1]));

    static {
        TEST_BUILD_INFO_LOCATION_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("module"));
        TEST_BUILD_INFO_LOCATION_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("representative_class"));

        TEST_BUILD_INFO_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("component"));
        TEST_BUILD_INFO_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), TEST_BUILD_INFO_LOCATION_PARSER::apply, new ParseField("locations"));

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

        // Log ifconfig output before SecurityManager is installed
        IfConfig.logIfNecessary();

        try {
            Enumeration<URL> urls = BootstrapForTesting.class.getClassLoader().getResources("META-INF/plugin-test-build-info.json");
            System.out.println("HERE0: " + urls.hasMoreElements());
            while (urls.hasMoreElements()) {
                try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, urls.nextElement().openStream())) {
                    TestBuildInfo testBuildInfo = TEST_BUILD_INFO_PARSER.parse(parser, null);
                    System.out.println("HERE1: " + testBuildInfo);
                }
            }
            System.out.println(
                "HERE2: " + BootstrapForTesting.class.getClassLoader().getResource("META-INF/plugin-test-build-info.json"));
            System.out.println(
                "HERE3: " + BootstrapForTesting.class.getResource("/META-INF/plugin-test-build-info.json"));
            System.out.println("HERE4: " + BootstrapForTesting.class.getProtectionDomain().getCodeSource().getLocation());
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    // does nothing, just easy way to make sure the class is loaded.
    public static void ensureInitialized() {}
}
