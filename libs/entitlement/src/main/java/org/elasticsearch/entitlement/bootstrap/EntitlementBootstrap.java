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
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Main entry point for firing up the Entitlements system.
 * Called by ES {@code initPhase2} to load the agent after setting up its prerequisites,
 * such as {@link BootstrapArgs} and some special module exports.
 */
public class EntitlementBootstrap {

    /**
     * A place to stash objects that the agent will need during its initialization
     */
    public record BootstrapArgs(
        PolicyManager policyManager,
        Map<String, Policy> pluginPolicies,
        PathLookup pathLookup,
        Set<Package> suppressFailureLogPackages
    ) {
        public BootstrapArgs {
            requireNonNull(policyManager);
            requireNonNull(pluginPolicies);
            requireNonNull(pathLookup);
            requireNonNull(suppressFailureLogPackages);
        }
    }

    private static BootstrapArgs bootstrapArgs;

    public static BootstrapArgs bootstrapArgs() {
        return bootstrapArgs;
    }

    /**
     * Activates entitlement checking. Once this method returns, calls to methods protected by Entitlements from classes without a valid
     * policy will throw {@link org.elasticsearch.entitlement.runtime.api.NotEntitledException}.
     * <p>
     * (Note: when we reference Elasticsearch "plugins" here, we generally also include Elasticsearch "modules".)
     *
     * @param policyManager
     * @param pluginPolicies             a map holding policies for plugins, by plugin name.
     * @param pathLookup
     * @param suppressFailureLogPackages packages for which we do not need or want to log Entitlements failures
     */
    public static void bootstrap(
        PolicyManager policyManager,
        Map<String, Policy> pluginPolicies,
        PathLookup pathLookup,
        Set<Package> suppressFailureLogPackages
    ) {
        logger.debug("Loading entitlement agent");
        if (EntitlementBootstrap.bootstrapArgs != null) {
            throw new IllegalStateException("plugin data is already set");
        }
        EntitlementBootstrap.bootstrapArgs = new BootstrapArgs(policyManager, pluginPolicies, pathLookup, suppressFailureLogPackages);
        exportInitializationToAgent();
        loadAgent(findAgentJar());
    }

    public static Path getUserHome() {
        String userHome = System.getProperty("user.home");
        if (userHome == null) {
            throw new IllegalStateException("user.home system property is required");
        }
        return PathUtils.get(userHome);
    }

    @SuppressForbidden(reason = "The VirtualMachine API is the only way to attach a java agent dynamically")
    private static void loadAgent(String agentPath) {
        try {
            VirtualMachine vm = VirtualMachine.attach(Long.toString(ProcessHandle.current().pid()));
            try {
                vm.loadAgent(agentPath, EntitlementInitialization.class.getName());
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

    public static String findAgentJar() {
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

    private static final Logger logger = LogManager.getLogger(EntitlementBootstrap.class);
}
