/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

public final class EntitlementBootstrap {
    private static volatile AgentParameters agentParameters = null;

    public static AgentParameters agentParameters() {
        return agentParameters;
    }

    public record AgentParameters(
        String bridgeLibrary,
        String runtimeLibrary
    ) {
        public AgentParameters {
            requireNonNull(bridgeLibrary);
            requireNonNull(runtimeLibrary);
        }
    }

    @SuppressForbidden(reason = "VirtualMachine.loadAgent is the only way to attach the agent dynamically")
    static void configure() {
        LibraryLocations libs = findEntitlementLibraries();
        logger.debug("Loading agent");
        agentParameters = new AgentParameters(libs.bridge(), libs.runtime());
        try {
            VirtualMachine vm = VirtualMachine.attach(Long.toString(ProcessHandle.current().pid()));
            try {
                vm.loadAgent(libs.agent());
            } finally {
                vm.detach();
            }
        } catch (AttachNotSupportedException | IOException | AgentLoadException | AgentInitializationException e) {
            throw new IllegalStateException("Unable to attach entitlement agent", e);
        }
    }

    private record LibraryLocations(String agent, String bridge, String runtime) { }

    private static LibraryLocations findEntitlementLibraries() {
        logger.debug("Finding entitlement libraries");
        return new LibraryLocations(
            findEntitlementJar("agent"),
            findEntitlementJar("bridge"),
            findEntitlementJar("runtime"));
    }

    private static String findEntitlementJar(String libraryName) {
        String propertyName = "es.entitlement." +  libraryName + "Jar";
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue != null) {
            return propertyValue;
        }

        Path dir = Path.of("lib", "tools", "entitlement-" + libraryName);
        if (dir.toFile().exists() == false) {
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
