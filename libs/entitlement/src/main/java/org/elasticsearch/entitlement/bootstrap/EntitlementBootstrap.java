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

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.initialization.EntitlementInitialization;
import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class EntitlementBootstrap {

    public record BootstrapArgs(
        Map<String, Policy> pluginPolicies,
        Function<Class<?>, String> pluginResolver,
        Path[] dataDirs,
        Path configDir,
        Path tempDir
    ) {
        public BootstrapArgs {
            requireNonNull(pluginPolicies);
            requireNonNull(pluginResolver);
            requireNonNull(dataDirs);
            if (dataDirs.length == 0) {
                throw new IllegalArgumentException("must provide at least one data directory");
            }
            requireNonNull(configDir);
            requireNonNull(tempDir);
        }
    }

    private static BootstrapArgs bootstrapArgs;

    public static BootstrapArgs bootstrapArgs() {
        return bootstrapArgs;
    }

    /**
     * Activates entitlement checking. Once this method returns, calls to methods protected by Entitlements from classes without a valid
     * policy will throw {@link org.elasticsearch.entitlement.runtime.api.NotEntitledException}.
     *
     * @param pluginPolicies a map holding policies for plugins (and modules), by plugin (or module) name.
     * @param pluginResolver a functor to map a Java Class to the plugin it belongs to (the plugin name).
     * @param dataDirs data directories for Elasticsearch
     * @param configDir the config directory for Elasticsearch
     * @param tempDir the temp directory for Elasticsearch
     */
    public static void bootstrap(
        Map<String, Policy> pluginPolicies,
        Function<Class<?>, String> pluginResolver,
        Path[] dataDirs,
        Path configDir,
        Path tempDir
    ) {
        logger.debug("Loading entitlement agent");
        if (EntitlementBootstrap.bootstrapArgs != null) {
            throw new IllegalStateException("plugin data is already set");
        }
        EntitlementBootstrap.bootstrapArgs = new BootstrapArgs(pluginPolicies, pluginResolver, dataDirs, configDir, tempDir);
        exportInitializationToAgent();
        loadAgent(findAgentJar());
        selfTest();
    }

    @SuppressForbidden(reason = "The VirtualMachine API is the only way to attach a java agent dynamically")
    private static void loadAgent(String agentPath) {
        try {
            VirtualMachine vm = VirtualMachine.attach(Long.toString(ProcessHandle.current().pid()));
            try {
                vm.loadAgent(agentPath);
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

    private static String findAgentJar() {
        String propertyName = "es.entitlement.agentJar";
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue != null) {
            return propertyValue;
        }

        Path dir = Path.of("lib", "entitlement-agent");
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

    /**
     * Attempt a few sensitive operations to ensure that some are permitted and some are forbidden.
     * <p>
     *
     * This serves two purposes:
     *
     * <ol>
     *     <li>
     *         a smoke test to make sure the entitlements system is not completely broken, and
     *     </li>
     *     <li>
     *         an early test of certain important operations so they don't fail later on at an awkward time.
     *     </li>
     * </ol>
     *
     * @throws IllegalStateException if the entitlements system can't prevent an unauthorized action of our choosing
     */
    private static void selfTest() {
        ensureCannotStartProcess(ProcessBuilder::start);
        // Try again with reflection
        ensureCannotStartProcess(EntitlementBootstrap::reflectiveStartProcess);
    }

    private static void ensureCannotStartProcess(CheckedConsumer<ProcessBuilder, ?> startProcess) {
        try {
            // The command doesn't matter; it doesn't even need to exist
            startProcess.accept(new ProcessBuilder(""));
        } catch (NotEntitledException e) {
            logger.debug("Success: Entitlement protection correctly prevented process creation");
            return;
        } catch (Exception e) {
            throw new IllegalStateException("Failed entitlement protection self-test", e);
        }
        throw new IllegalStateException("Entitlement protection self-test was incorrectly permitted");
    }

    private static void reflectiveStartProcess(ProcessBuilder pb) throws Exception {
        try {
            var start = ProcessBuilder.class.getMethod("start");
            start.invoke(pb);
        } catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }

    private static final Logger logger = LogManager.getLogger(EntitlementBootstrap.class);
}
