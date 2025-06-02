/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bootstrap;

import org.elasticsearch.entitlement.initialization.EntitlementInitialization;
import org.elasticsearch.entitlement.initialization.TestEntitlementInitialization;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class TestEntitlementBootstrap {

    private static final Logger logger = LogManager.getLogger(TestEntitlementBootstrap.class);

    /**
     * Activates entitlement checking in tests.
     */
    public static void bootstrap() {
        EntitlementInitialization.initializeArgs = new EntitlementInitialization.InitializeArgs(
            null,
            Map.of(),
            c -> { throw new IllegalStateException("Not yet implemented"); },
            new TestPathLookup(),
            Map.of(),
            Set.of()
        );
        logger.debug("Loading entitlement agent");
        EntitlementBootstrap.loadAgent(EntitlementBootstrap.findAgentJar(), TestEntitlementInitialization.class.getName());
    }

    private record TestPathLookup() implements PathLookup {
        @Override
        public Path pidFile() {
            return null;
        }

        @Override
        public Stream<Path> getBaseDirPaths(BaseDir baseDir) {
            return Stream.empty();
        }

        @Override
        public Stream<Path> resolveRelativePaths(BaseDir baseDir, Path relativePath) {
            return Stream.empty();
        }

        @Override
        public Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName) {
            return Stream.empty();
        }

    }
}
