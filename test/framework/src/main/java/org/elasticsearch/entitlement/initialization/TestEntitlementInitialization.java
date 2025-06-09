/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.bootstrap.TestBuildInfo;
import org.elasticsearch.bootstrap.TestBuildInfoParser;
import org.elasticsearch.bootstrap.TestScopeResolver;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.PolicyParser;
import org.elasticsearch.entitlement.runtime.policy.TestPolicyManager;
import org.elasticsearch.plugins.PluginDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.entitlement.initialization.EntitlementInitialization.initInstrumentation;

/**
 * Test-specific version of {@code EntitlementInitialization}
 */
public class TestEntitlementInitialization {

    private static ElasticsearchEntitlementChecker checker;
    public static InitializeArgs initializeArgs;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return checker;
    }

    public static void initialize(Instrumentation inst) throws Exception {
        checker = EntitlementInitialization.initChecker(createPolicyManager(initializeArgs.pathLookup()));
        initInstrumentation(inst);
    }

    public record InitializeArgs(PathLookup pathLookup) {}

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

    private static PolicyManager createPolicyManager(PathLookup pathLookup) throws IOException {

        var pluginsTestBuildInfo = TestBuildInfoParser.parseAllPluginTestBuildInfo();
        var serverTestBuildInfo = TestBuildInfoParser.parseServerTestBuildInfo();
        var scopeResolver = TestScopeResolver.createScopeResolver(serverTestBuildInfo, pluginsTestBuildInfo);
        List<String> pluginNames = pluginsTestBuildInfo.stream().map(TestBuildInfo::component).toList();

        var pluginDescriptors = parsePluginsDescriptors(pluginNames);
        var pluginsData = pluginDescriptors.stream()
            .map(descriptor -> new TestPluginData(descriptor.getName(), descriptor.isModular(), false))
            .toList();
        Map<String, Policy> pluginPolicies = parsePluginsPolicies(pluginsData);

        FilesEntitlementsValidation.validate(pluginPolicies, pathLookup);

        return new TestPolicyManager(
            HardcodedEntitlements.serverPolicy(null, null),
            HardcodedEntitlements.agentEntitlements(),
            pluginPolicies,
            scopeResolver,
            Map.of(),
            pathLookup
        );
    }

}
