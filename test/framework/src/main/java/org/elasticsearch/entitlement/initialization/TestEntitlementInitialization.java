/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.PolicyParser;
import org.elasticsearch.plugins.PluginDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Test-only version of {@code EntitlementInitialization}
 */
public class TestEntitlementInitialization {

    private static ElasticsearchEntitlementChecker checker;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return checker;
    }

    public static void initialize(Instrumentation inst) throws Exception {
        checker = EntitlementInitialization.initChecker(inst, createPolicyManager());
    }

    private record TestPluginData(String pluginName, boolean isModular, boolean isExternalPlugin) {}

    private static Map<String, Policy> parsePluginsPolicies(List<TestPluginData> pluginsData) {
        Map<String, Policy> policies = new HashMap<>();
        for (var pluginData : pluginsData) {
            String pluginName = pluginData.pluginName();
            var resourceName = Strings.format("META-INF/es-plugins/%s/entitlement-policy.yaml", pluginName);

            var resource = TestEntitlementInitialization.class.getClassLoader().getResource(resourceName);
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
            var resource = TestEntitlementInitialization.class.getClassLoader().getResource(resourceName);
            if (resource != null) {
                try (var inputStream = getStream(resource)) {
                    descriptors.add(PluginDescriptor.readInternalDescriptor(inputStream));
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

    private static PolicyManager createPolicyManager() {

        // TODO: uncomment after merging https://github.com/elastic/elasticsearch/pull/127719
        // var pluginsTestBuildInfo = TestBuildInfoParser.parseAllPluginTestBuildInfo();
        // var serverTestBuildInfo = TestBuildInfoParser.parseServerTestBuildInfo();
        Function<Class<?>, PolicyManager.PolicyScope> scopeResolver = null; // TestScopeResolver.createScopeResolver(serverTestBuildInfo,
                                                                            // pluginsTestBuildInfo);
        List<String> pluginNames = List.of(); // = pluginsTestBuildInfo.stream().map(TestBuildInfo::componentName).toList();

        var pluginDescriptors = parsePluginsDescriptors(pluginNames);
        var pluginsData = pluginDescriptors.stream()
            .map(descriptor -> new TestPluginData(descriptor.getName(), descriptor.isModular(), false))
            .toList();
        Map<String, Policy> pluginPolicies = parsePluginsPolicies(pluginsData);

        // TODO: create here the test pathLookup
        PathLookup pathLookup = null;

        FilesEntitlementsValidation.validate(pluginPolicies, pathLookup);

        return new PolicyManager(
            HardcodedEntitlements.serverPolicy(null, null),
            HardcodedEntitlements.agentEntitlements(),
            pluginPolicies,
            scopeResolver,
            Map.of(),
            null, // TODO: this will need to change -- encapsulate it when we extract isTriviallyAllowed
            pathLookup,
            Set.of()
        );
    }
}
