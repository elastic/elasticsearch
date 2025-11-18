/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bootstrap;

import org.apache.lucene.tests.mockfile.FilterPath;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir;
import org.elasticsearch.entitlement.runtime.policy.TestPolicyManager;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.Closeable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.env.Environment.PATH_DATA_SETTING;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.env.Environment.PATH_REPO_SETTING;
import static org.elasticsearch.env.Environment.PATH_SHARED_DATA_SETTING;

public class TestEntitlementsRule implements TestRule {
    private static final Logger logger = LogManager.getLogger(TestEntitlementsRule.class);

    private static final AtomicBoolean active = new AtomicBoolean(false);
    private final TestPolicyManager policyManager;
    private final TestPathLookup pathLookup;

    public TestEntitlementsRule() {
        policyManager = TestEntitlementBootstrap.testPolicyManager();
        pathLookup = TestEntitlementBootstrap.testPathLookup();
        assert (policyManager == null) == (pathLookup == null);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        assert description.isSuite() : "must be used as ClassRule";

        // class / suite level
        boolean withoutEntitlements = description.getAnnotation(ESTestCase.WithoutEntitlements.class) != null;
        boolean withEntitlementsOnTestCode = description.getAnnotation(ESTestCase.WithEntitlementsOnTestCode.class) != null;
        var entitledPackages = description.getAnnotation(ESTestCase.EntitledTestPackages.class);

        if (policyManager != null) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    if (active.compareAndSet(false, true)) {
                        try {
                            pathLookup.reset();
                            policyManager.setActive(false == withoutEntitlements);
                            policyManager.setTriviallyAllowingTestCode(false == withEntitlementsOnTestCode);
                            if (entitledPackages != null) {
                                assert entitledPackages.value().length > 0 : "No test packages specified in @EntitledTestPackages";
                                policyManager.setEntitledTestPackages(entitledPackages.value());
                            } else {
                                policyManager.setEntitledTestPackages();
                            }
                            policyManager.clearModuleEntitlementsCache();
                            // evaluate the suite
                            base.evaluate();
                        } finally {
                            pathLookup.reset();
                            policyManager.resetAfterTest();
                            active.set(false);
                        }
                    } else {
                        throw new AssertionError("TestPolicyManager doesn't support test isolation, test suits cannot be run in parallel");
                    }
                }
            };
        } else if (withEntitlementsOnTestCode) {
            throw new AssertionError(
                "Cannot use @WithEntitlementsOnTestCode on tests that are not configured to use entitlements for testing"
            );
        } else {
            return base;
        }
    }

    /**
     * Temporarily adds node paths based entitlements based on a node's {@code settings} and {@code configPath}
     * until the returned handle is closed.
     * @see PathLookup
     */
    public Closeable addEntitledNodePaths(Settings settings, Path configPath) {
        if (policyManager == null) {
            return () -> {}; // noop if not running with entitlements
        }

        var unwrappedConfigPath = configPath;
        while (unwrappedConfigPath instanceof FilterPath fPath) {
            unwrappedConfigPath = fPath.getDelegate();
        }
        EntitledNodePaths entitledNodePaths = new EntitledNodePaths(settings, unwrappedConfigPath, this::removeEntitledNodePaths);
        addEntitledNodePaths(entitledNodePaths);
        return entitledNodePaths;
    }

    /**
     * Revoke all entitled node paths.
     */
    public void revokeAllEntitledNodePaths() {
        if (policyManager != null) {
            pathLookup.reset();
            policyManager.clearModuleEntitlementsCache();
        }
    }

    private record EntitledNodePaths(Settings settings, Path configPath, Consumer<EntitledNodePaths> onClose) implements Closeable {
        private Path homeDir() {
            return absolutePath(PATH_HOME_SETTING.get(settings));
        }

        private Path configDir() {
            return configPath != null ? configPath : homeDir().resolve("config");
        }

        private Path[] dataDirs() {
            List<String> dataDirs = PATH_DATA_SETTING.get(settings);
            return dataDirs.isEmpty()
                ? new Path[] { homeDir().resolve("data") }
                : dataDirs.stream().map(EntitledNodePaths::absolutePath).toArray(Path[]::new);
        }

        private Path[] sharedDataDir() {
            String sharedDataDir = PATH_SHARED_DATA_SETTING.get(settings);
            return Strings.hasText(sharedDataDir) ? new Path[] { absolutePath(sharedDataDir) } : new Path[0];
        }

        private Path[] repoDirs() {
            return PATH_REPO_SETTING.get(settings).stream().map(EntitledNodePaths::absolutePath).toArray(Path[]::new);
        }

        @SuppressForbidden(reason = "must be resolved using the default file system, rather then the mocked test file system")
        private static Path absolutePath(String path) {
            return Paths.get(path).toAbsolutePath().normalize();
        }

        @Override
        public void close() {
            // wipePendingDataDirectories in tests requires entitlement delegation to work as this uses server's FileSystemUtils.
            // until ES-10920 is solved, node grants cannot be removed until the test suite completes unless explicitly removing all node
            // grants using revokeNodeGrants where feasible.
            // onClose.accept(this);
        }

        @Override
        public String toString() {
            return Strings.format(
                "EntitledNodePaths[configDir=%s, dataDirs=%s, sharedDataDir=%s, repoDirs=%s]",
                configDir(),
                dataDirs(),
                sharedDataDir(),
                repoDirs()
            );
        }
    }

    private void addEntitledNodePaths(EntitledNodePaths entitledNodePaths) {
        logger.debug("Adding {}", entitledNodePaths);
        pathLookup.add(BaseDir.CONFIG, entitledNodePaths.configDir());
        pathLookup.add(BaseDir.DATA, entitledNodePaths.dataDirs());
        pathLookup.add(BaseDir.SHARED_DATA, entitledNodePaths.sharedDataDir());
        pathLookup.add(BaseDir.SHARED_REPO, entitledNodePaths.repoDirs());
        policyManager.clearModuleEntitlementsCache();
    }

    private void removeEntitledNodePaths(EntitledNodePaths entitledNodePaths) {
        logger.debug("Removing {}", entitledNodePaths);
        pathLookup.remove(BaseDir.CONFIG, entitledNodePaths.configDir());
        pathLookup.remove(BaseDir.DATA, entitledNodePaths.dataDirs());
        pathLookup.remove(BaseDir.SHARED_DATA, entitledNodePaths.sharedDataDir());
        pathLookup.remove(BaseDir.SHARED_REPO, entitledNodePaths.repoDirs());
        policyManager.clearModuleEntitlementsCache();
    }
}
