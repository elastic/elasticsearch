/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bootstrap;

import org.elasticsearch.bootstrap.TestBuildInfo;
import org.elasticsearch.bootstrap.TestBuildInfoParser;
import org.elasticsearch.bootstrap.TestScopeResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.config.MainInstrumentationProvider;
import org.elasticsearch.entitlement.initialization.EntitlementInitialization;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyCheckerImpl;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.PolicyParser;
import org.elasticsearch.entitlement.runtime.policy.TestPolicyManager;
import org.elasticsearch.entitlement.runtime.registry.InstrumentationRegistryImpl;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.PluginDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;

public class TestEntitlementBootstrap {
    private static final Logger logger = LogManager.getLogger(TestEntitlementBootstrap.class);

    private static TestPathLookup TEST_PATH_LOOKUP;
    private static TestPolicyManager POLICY_MANAGER;

    /**
     * Activates entitlement checking in tests.
     */
    public static void bootstrap(Path tempDir) throws IOException {
        if (isEnabledForTests() == false) {
            return;
        }
        assert POLICY_MANAGER == null && TEST_PATH_LOOKUP == null : "Test entitlement bootstrap called multiple times";
        TEST_PATH_LOOKUP = new TestPathLookup(tempDir);
        POLICY_MANAGER = createPolicyManager(TEST_PATH_LOOKUP);
        loadAgent(POLICY_MANAGER, TEST_PATH_LOOKUP);
    }

    public static boolean isEnabledForTests() {
        return Booleans.parseBoolean(System.getProperty("es.entitlement.enableForTests", "false"));
    }

    static TestPolicyManager testPolicyManager() {
        return POLICY_MANAGER;
    }

    static TestPathLookup testPathLookup() {
        return TEST_PATH_LOOKUP;
    }

    private static void loadAgent(PolicyManager policyManager, PathLookup pathLookup) {
        logger.debug("Loading entitlement agent");
        PolicyCheckerImpl policyChecker = createPolicyChecker(Set.of(), policyManager, pathLookup);
        InstrumentationRegistryImpl instrumentationRegistry = new InstrumentationRegistryImpl(policyChecker);
        EntitlementInitialization.initializeArgs = new EntitlementInitialization.InitializeArgs(
            pathLookup,
            Set.of(),
            policyChecker,
            instrumentationRegistry
        );
        new MainInstrumentationProvider().init(instrumentationRegistry);
        EntitlementBootstrap.loadAgent(EntitlementBootstrap.findAgentJar(), EntitlementInitialization.class.getName());
    }

    private static TestPolicyManager createPolicyManager(PathLookup pathLookup) throws IOException {
        var pluginsTestBuildInfo = TestBuildInfoParser.parseAllPluginTestBuildInfo();
        var serverTestBuildInfo = TestBuildInfoParser.parseServerTestBuildInfo();
        List<String> pluginNames = pluginsTestBuildInfo.stream().map(TestBuildInfo::component).toList();

        var pluginDescriptors = parsePluginsDescriptors(pluginNames);
        Set<String> modularPlugins = pluginDescriptors.stream()
            .filter(PluginDescriptor::isModular)
            .map(PluginDescriptor::getName)
            .collect(toSet());
        var scopeResolver = TestScopeResolver.createScopeResolver(serverTestBuildInfo, pluginsTestBuildInfo, modularPlugins);
        var pluginsData = pluginDescriptors.stream()
            .map(descriptor -> new TestPluginData(descriptor.getName(), descriptor.isModular(), false))
            .toList();
        Map<String, Policy> pluginPolicies = parsePluginsPolicies(pluginsData);

        String separator = System.getProperty("path.separator");

        // In production, plugins would have access to their respective bundle directories,
        // and so they'd be able to read from their jars. In testing, we approximate this
        // by considering the entire classpath to be "source paths" of all plugins. This
        // also has the effect of granting read access to everything on the test-only classpath,
        // which is fine, because any entitlement errors there could only be false positives.
        String classPathProperty = System.getProperty("java.class.path");

        Set<Path> classPathEntries;
        if (classPathProperty == null) {
            classPathEntries = Set.of();
        } else {
            classPathEntries = Arrays.stream(classPathProperty.split(separator)).map(PathUtils::get).collect(toCollection(TreeSet::new));
        }
        FilesEntitlementsValidation.validate(pluginPolicies, pathLookup);

        String testOnlyPathString = System.getenv("es.entitlement.testOnlyPath");
        Set<URI> testOnlyClassPath;
        if (testOnlyPathString == null) {
            testOnlyClassPath = Set.of();
        } else {
            testOnlyClassPath = Arrays.stream(testOnlyPathString.split(separator))
                .map(PathUtils::get)
                .map(Path::toUri)
                .collect(toCollection(TreeSet::new));
        }

        return new TestPolicyManager(
            HardcodedEntitlements.serverPolicy(null, null),
            HardcodedEntitlements.agentEntitlements(),
            pluginPolicies,
            scopeResolver,
            pathLookup,
            classPathEntries,
            testOnlyClassPath
        );
    }

    private static PolicyCheckerImpl createPolicyChecker(
        Set<Package> suppressFailureLogPackages,
        PolicyManager policyManager,
        PathLookup pathLookup
    ) {
        return new PolicyCheckerImpl(suppressFailureLogPackages, EntitlementBootstrap.ENTITLEMENTS_MODULE, policyManager, pathLookup);
    }

    private static Map<String, Policy> parsePluginsPolicies(List<TestPluginData> pluginsData) {
        Map<String, Policy> policies = new HashMap<>();
        for (var pluginData : pluginsData) {
            String pluginName = pluginData.pluginName();
            var resourceName = Strings.format("META-INF/es-plugins/%s/entitlement-policy.yaml", pluginName);

            var resource = EntitlementInitialization.class.getClassLoader().getResource(resourceName);
            if (resource != null) {
                try (var inputStream = getStream(resource)) {
                    policies.put(pluginName, new PolicyParser(inputStream, pluginName, pluginData.isExternalPlugin()).parsePolicy());
                } catch (IOException e) {
                    throw new IllegalArgumentException(Strings.format("Cannot read policy for plugin [%s]", pluginName), e);
                }
            }
        }
        return policies;
    }

    private static List<PluginDescriptor> parsePluginsDescriptors(List<String> pluginNames) {
        List<PluginDescriptor> descriptors = new ArrayList<>();
        for (var pluginName : pluginNames) {
            var resourceName = Strings.format("META-INF/es-plugins/%s/plugin-descriptor.properties", pluginName);
            var resource = EntitlementInitialization.class.getClassLoader().getResource(resourceName);
            if (resource != null) {
                try (var inputStream = getStream(resource)) {
                    descriptors.add(PluginDescriptor.readInternalDescriptorFromStream(inputStream));
                } catch (IOException e) {
                    throw new IllegalArgumentException(Strings.format("Cannot read descriptor for plugin [%s]", pluginName), e);
                }
            }
        }
        return descriptors;
    }

    @SuppressForbidden(reason = "URLs from class loader")
    private static InputStream getStream(URL resource) throws IOException {
        return resource.openStream();
    }

    private record TestPluginData(String pluginName, boolean isModular, boolean isExternalPlugin) {}

}
