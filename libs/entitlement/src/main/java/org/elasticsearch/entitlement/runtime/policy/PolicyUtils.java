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
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteSystemPropertiesEntitlement;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ALL_UNNAMED;

public class PolicyUtils {

    private static final Logger logger = LogManager.getLogger(PolicyUtils.class);

    public record PluginData(Path pluginPath, boolean isModular, boolean isExternalPlugin) {
        public PluginData {
            requireNonNull(pluginPath);
        }
    }

    private static final String POLICY_FILE_NAME = "entitlement-policy.yaml";

    public static Map<String, Policy> createPluginPolicies(Collection<PluginData> pluginData, Map<String, String> overrides, String version)
        throws IOException {
        Map<String, Policy> pluginPolicies = new HashMap<>(pluginData.size());
        for (var entry : pluginData) {
            Path pluginRoot = entry.pluginPath();
            String pluginName = pluginRoot.getFileName().toString();
            final Set<String> moduleNames = getModuleNames(pluginRoot, entry.isModular());

            var overriddenPolicy = parseEncodedPolicyIfExists(
                overrides.get(pluginName),
                version,
                entry.isExternalPlugin(),
                pluginName,
                moduleNames
            );
            if (overriddenPolicy != null) {
                pluginPolicies.put(pluginName, overriddenPolicy);
            } else {
                Path policyFile = pluginRoot.resolve(POLICY_FILE_NAME);
                var policy = parsePolicyIfExists(pluginName, policyFile, entry.isExternalPlugin());
                validatePolicyScopes(pluginName, policy, moduleNames, policyFile.toString());
                pluginPolicies.put(pluginName, policy);
            }
        }
        return pluginPolicies;
    }

    public static Policy parseEncodedPolicyIfExists(
        String encodedPolicy,
        String version,
        boolean externalPlugin,
        String layerName,
        Set<String> moduleNames
    ) {
        if (encodedPolicy != null) {
            try {
                var versionedPolicy = decodeEncodedPolicy(encodedPolicy, layerName, externalPlugin);
                validatePolicyScopes(layerName, versionedPolicy.policy(), moduleNames, "<patch>");

                // Empty versions defaults to "any"
                if (versionedPolicy.versions().isEmpty() || versionedPolicy.versions().contains(version)) {
                    logger.info("Using policy patch for layer [{}]", layerName);
                    return versionedPolicy.policy();
                } else {
                    logger.warn(
                        "Found a policy patch with version mismatch. The patch will not be applied. "
                            + "Layer [{}]; policy versions [{}]; current version [{}]",
                        layerName,
                        String.join(",", versionedPolicy.versions()),
                        version
                    );
                }
            } catch (Exception ex) {
                logger.warn(
                    Strings.format("Found a policy patch with invalid content. The patch will not be applied. Layer [%s]", layerName),
                    ex
                );
            }
        }
        return null;
    }

    static VersionedPolicy decodeEncodedPolicy(String base64String, String layerName, boolean isExternalPlugin) throws IOException {
        byte[] policyDefinition = Base64.getDecoder().decode(base64String);
        return new PolicyParser(new ByteArrayInputStream(policyDefinition), layerName, isExternalPlugin).parseVersionedPolicy();
    }

    private static void validatePolicyScopes(String layerName, Policy policy, Set<String> moduleNames, String policyLocation) {
        // TODO: should this check actually be part of the parser?
        for (Scope scope : policy.scopes()) {
            if (moduleNames.contains(scope.moduleName()) == false) {
                throw new IllegalStateException(
                    Strings.format(
                        "Invalid module name in policy: layer [%s] does not have module [%s]; available modules [%s]; policy path [%s]",
                        layerName,
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

    public static List<Scope> mergeScopes(List<Scope> mainScopes, List<Scope> additionalScopes) {
        var result = new ArrayList<Scope>();
        var additionalScopesMap = additionalScopes.stream().collect(Collectors.toMap(Scope::moduleName, Scope::entitlements));
        for (var mainScope : mainScopes) {
            List<Entitlement> additionalEntitlements = additionalScopesMap.remove(mainScope.moduleName());
            if (additionalEntitlements == null) {
                result.add(mainScope);
            } else {
                result.add(new Scope(mainScope.moduleName(), mergeEntitlements(mainScope.entitlements(), additionalEntitlements)));
            }
        }

        for (var remainingEntry : additionalScopesMap.entrySet()) {
            result.add(new Scope(remainingEntry.getKey(), remainingEntry.getValue()));
        }
        return result;
    }

    static List<Entitlement> mergeEntitlements(List<Entitlement> a, List<Entitlement> b) {
        Map<Class<? extends Entitlement>, Entitlement> entitlementMap = a.stream()
            .collect(Collectors.toMap(Entitlement::getClass, Function.identity()));

        for (var entitlement : b) {
            entitlementMap.merge(entitlement.getClass(), entitlement, PolicyUtils::mergeEntitlement);
        }
        return entitlementMap.values().stream().toList();
    }

    static Entitlement mergeEntitlement(Entitlement entitlement1, Entitlement entitlement2) {
        return switch (entitlement1) {
            case FilesEntitlement e -> merge(e, (FilesEntitlement) entitlement2);
            case WriteSystemPropertiesEntitlement e -> merge(e, (WriteSystemPropertiesEntitlement) entitlement2);
            default -> entitlement1;
        };
    }

    private static FilesEntitlement merge(FilesEntitlement a, FilesEntitlement b) {
        return new FilesEntitlement(Stream.concat(a.filesData().stream(), b.filesData().stream()).distinct().toList());
    }

    private static WriteSystemPropertiesEntitlement merge(WriteSystemPropertiesEntitlement a, WriteSystemPropertiesEntitlement b) {
        return new WriteSystemPropertiesEntitlement(
            Stream.concat(a.properties().stream(), b.properties().stream()).collect(Collectors.toUnmodifiableSet())
        );
    }
}
