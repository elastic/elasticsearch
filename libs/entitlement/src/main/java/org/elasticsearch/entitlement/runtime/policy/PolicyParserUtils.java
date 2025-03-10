/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ALL_UNNAMED;

public class PolicyParserUtils {

    private static final Logger logger = LogManager.getLogger(PolicyParserUtils.class);

    public record PluginData(Path pluginPath, boolean isModular, boolean isExternalPlugin) {
        public PluginData {
            requireNonNull(pluginPath);
        }
    }

    private static final String POLICY_FILE_NAME = "entitlement-policy.yaml";

    public static final String POLICY_OVERRIDE_PREFIX = "es.entitlements.policy.";

    public static Map<String, Policy> createPluginPolicies(Collection<PluginData> pluginData, Map<String, String> overrides)
        throws IOException {
        Map<String, Policy> pluginPolicies = new HashMap<>(pluginData.size());
        for (var entry : pluginData) {
            Path pluginRoot = entry.pluginPath();
            String pluginName = pluginRoot.getFileName().toString();
            final Set<String> moduleNames = getModuleNames(pluginRoot, entry.isModular());

            final Policy policy;
            var policyOverride = overrides.get(pluginName);
            if (policyOverride != null) {
                logger.info("Using policy override for plugin [{}]", pluginName);
                policy = decodeOverriddenPluginPolicy(policyOverride, pluginName, entry.isExternalPlugin());
                validatePolicyScopes(pluginName, policy, moduleNames, "<override>");
            } else {
                Path policyFile = pluginRoot.resolve(POLICY_FILE_NAME);
                policy = parsePolicyIfExists(pluginName, policyFile, entry.isExternalPlugin());
                validatePolicyScopes(pluginName, policy, moduleNames, policyFile.toString());
            }
            pluginPolicies.put(pluginName, policy);
        }
        return pluginPolicies;
    }

    static Policy decodeOverriddenPluginPolicy(String base64String, String pluginName, boolean isExternalPlugin) throws IOException {
        byte[] policyDefinition = Base64.getDecoder().decode(base64String);
        return new PolicyParser(new ByteArrayInputStream(policyDefinition), pluginName, isExternalPlugin).parsePolicy();
    }

    private static void validatePolicyScopes(String pluginName, Policy policy, Set<String> moduleNames, String policyLocation) {
        // TODO: should this check actually be part of the parser?
        for (Scope scope : policy.scopes()) {
            if (moduleNames.contains(scope.moduleName()) == false) {
                throw new IllegalStateException(
                    Strings.format(
                        "Invalid module name in policy: plugin [%s] does not have module [%s]; available modules [%s]; policy path [%s]",
                        pluginName,
                        scope.moduleName(),
                        String.join(", ", moduleNames),
                        policyLocation
                    )
                );
            }
        }
    }

    private static Policy parsePolicyIfExists(String pluginName, Path policyFile, boolean isExternalPlugin) throws IOException {
        if (Files.exists(policyFile)) {
            return new PolicyParser(Files.newInputStream(policyFile, StandardOpenOption.READ), pluginName, isExternalPlugin).parsePolicy();
        }
        return new Policy(pluginName, List.of());
    }

    private static Set<String> getModuleNames(Path pluginRoot, boolean isModular) {
        if (isModular) {
            ModuleFinder moduleFinder = ModuleFinder.of(pluginRoot);
            Set<ModuleReference> moduleReferences = moduleFinder.findAll();

            return moduleReferences.stream().map(mr -> mr.descriptor().name()).collect(Collectors.toUnmodifiableSet());
        }
        // When isModular == false we use the same "ALL-UNNAMED" constant as the JDK to indicate (any) unnamed module for this plugin
        return Set.of(ALL_UNNAMED);
    }

}
