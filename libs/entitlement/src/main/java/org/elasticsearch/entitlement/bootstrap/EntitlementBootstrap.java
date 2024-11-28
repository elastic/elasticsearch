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

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.entitlement.initialization.EntitlementInitialization;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;

public class EntitlementBootstrap {

    public record BootstrapArgs(Collection<Tuple<Path, Boolean>> pluginData, Function<Class<?>, String> pluginResolver) {}

    private static BootstrapArgs bootstrapArgs;

    public static BootstrapArgs bootstrapArgs() {
        return bootstrapArgs;
    }

    /**
     * Activates entitlement checking. Once this method returns, calls to methods protected by Entitlements from classes without a valid
     * policy will throw {@link org.elasticsearch.entitlement.runtime.api.NotEntitledException}.
     * @param pluginData a collection of (plugin path, boolean), that holds the paths of all the installed Elasticsearch modules and
     *                   plugins, and whether they are Java modular or not.
     * @param pluginResolver a functor to map a Java Class to the plugin it belongs to (the plugin name).
     */
    public static void bootstrap(Collection<Tuple<Path, Boolean>> pluginData, Function<Class<?>, String> pluginResolver) {
        logger.debug("Loading entitlement agent");
        if (EntitlementBootstrap.bootstrapArgs != null) {
            throw new IllegalStateException("plugin data is already set");
        }
        EntitlementBootstrap.bootstrapArgs = new BootstrapArgs(Objects.requireNonNull(pluginData), Objects.requireNonNull(pluginResolver));
        exportInitializationToAgent();
        loadAgent(findAgentJar());
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

    private static final Logger logger = LogManager.getLogger(EntitlementBootstrap.class);
}
