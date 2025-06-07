/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bootstrap;

import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.initialization.EntitlementInitialization;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.PathLookupImpl;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class EntitlementBootstrap {

    /**
     * Main entry point that activates entitlement checking. Once this method returns,
     * calls to methods protected by entitlements from classes without a valid
     * policy will throw {@link org.elasticsearch.entitlement.runtime.api.NotEntitledException}.
     *
     * @param serverPolicyPatch a policy with additional entitlements to patch the embedded server layer policy
     * @param pluginPolicies a map holding policies for plugins (and modules), by plugin (or module) name.
     * @param scopeResolver a functor to map a Java Class to the component and module it belongs to.
     * @param settingResolver a functor to resolve a setting name pattern for one or more Elasticsearch settings.
     * @param dataDirs       data directories for Elasticsearch
     * @param sharedRepoDirs shared repository directories for Elasticsearch
     * @param configDir      the config directory for Elasticsearch
     * @param libDir         the lib directory for Elasticsearch
     * @param modulesDir     the directory where Elasticsearch modules are
     * @param pluginsDir     the directory where plugins are installed for Elasticsearch
     * @param pluginSourcePaths    a map holding the path to each plugin or module jars, by plugin (or module) name.
     * @param tempDir        the temp directory for Elasticsearch
     * @param logsDir        the log directory for Elasticsearch
     * @param pidFile        path to a pid file for Elasticsearch, or {@code null} if one was not specified
     * @param suppressFailureLogPackages   packages for which we do not need or want to log Entitlements failures
     */
    public static void bootstrap(
        Policy serverPolicyPatch,
        Map<String, Policy> pluginPolicies,
        Function<Class<?>, PolicyManager.PolicyScope> scopeResolver,
        Function<String, Stream<String>> settingResolver,
        Path[] dataDirs,
        Path[] sharedRepoDirs,
        Path configDir,
        Path libDir,
        Path modulesDir,
        Path pluginsDir,
        Map<String, Collection<Path>> pluginSourcePaths,
        Path logsDir,
        Path tempDir,
        Path pidFile,
        Set<Package> suppressFailureLogPackages
    ) {
        logger.debug("Loading entitlement agent");
        if (EntitlementInitialization.initializeArgs != null) {
            throw new IllegalStateException("initialization data is already set");
        }
        PathLookupImpl pathLookup = new PathLookupImpl(
            getUserHome(),
            configDir,
            dataDirs,
            sharedRepoDirs,
            libDir,
            modulesDir,
            pluginsDir,
            logsDir,
            tempDir,
            pidFile,
            settingResolver
        );
        EntitlementInitialization.initializeArgs = new EntitlementInitialization.InitializeArgs(
            pathLookup,
            suppressFailureLogPackages,
            createPolicyManager(pluginPolicies, pathLookup, serverPolicyPatch, scopeResolver, pluginSourcePaths)
        );
        exportInitializationToAgent();
        loadAgent(findAgentJar(), EntitlementInitialization.class.getName());
    }

    private static Path getUserHome() {
        String userHome = System.getProperty("user.home");
        if (userHome == null) {
            throw new IllegalStateException("user.home system property is required");
        }
        return PathUtils.get(userHome);
    }

    @SuppressForbidden(reason = "The VirtualMachine API is the only way to attach a java agent dynamically")
    static void loadAgent(String agentPath, String entitlementInitializationClassName) {
        try {
            VirtualMachine vm = VirtualMachine.attach(Long.toString(ProcessHandle.current().pid()));
            try {
                vm.loadAgent(agentPath, entitlementInitializationClassName);
            } finally {
                vm.detach();
            }
        } catch (AttachNotSupportedException | IOException | AgentLoadException | AgentInitializationException e) {
            throw new IllegalStateException("Unable to attach entitlement agent", e);
        }
    }

    private static void exportInitializationToAgent() {
        String initPkg = EntitlementInitialization.class.getPackageName();
        // agent will live in unnamed module
        Module unnamedModule = ClassLoader.getSystemClassLoader().getUnnamedModule();
        EntitlementInitialization.class.getModule().addExports(initPkg, unnamedModule);
    }

    static String findAgentJar() {
        String propertyName = "es.entitlement.agentJar";
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue != null) {
            return propertyValue;
        }

        Path esHome = Path.of(System.getProperty("es.path.home"));
        Path dir = esHome.resolve("lib/entitlement-agent");
        if (Files.exists(dir) == false) {
            throw new IllegalStateException("Directory for entitlement jar does not exist: " + dir);
        }
        try (var s = Files.list(dir)) {
            var candidates = s.limit(2).toList();
            if (candidates.size() != 1) {
                throw new IllegalStateException("Expected one jar in " + dir + "; found " + candidates.size());
            }
            return candidates.get(0).toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to list entitlement jars in: " + dir, e);
        }
    }

    private static PolicyManager createPolicyManager(
        Map<String, Policy> pluginPolicies,
        PathLookup pathLookup,
        Policy serverPolicyPatch,
        Function<Class<?>, PolicyManager.PolicyScope> scopeResolver,
        Map<String, Collection<Path>> pluginSourcePaths
    ) {
        FilesEntitlementsValidation.validate(pluginPolicies, pathLookup);

        return new PolicyManager(
            HardcodedEntitlements.serverPolicy(pathLookup.pidFile(), serverPolicyPatch),
            HardcodedEntitlements.agentEntitlements(),
            pluginPolicies,
            scopeResolver,
            pluginSourcePaths,
            pathLookup
        );
    }

    private static final Logger logger = LogManager.getLogger(EntitlementBootstrap.class);
}
