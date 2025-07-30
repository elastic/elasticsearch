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
import org.elasticsearch.bootstrap.TestBuildInfo;
import org.elasticsearch.bootstrap.TestBuildInfoParser;
import org.elasticsearch.bootstrap.TestScopeResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.initialization.EntitlementInitialization;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.PolicyParser;
import org.elasticsearch.entitlement.runtime.policy.TestPathLookup;
import org.elasticsearch.entitlement.runtime.policy.TestPolicyManager;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.test.ESTestCase;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.TEMP;
import static org.elasticsearch.env.Environment.PATH_DATA_SETTING;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.env.Environment.PATH_REPO_SETTING;
import static org.elasticsearch.env.Environment.PATH_SHARED_DATA_SETTING;

public class TestEntitlementsRule implements TestRule {
    private static final Logger logger = LogManager.getLogger(TestEntitlementsRule.class);

    private static final Map<BaseDir, Collection<Path>> BASE_DIR_PATHS = new ConcurrentHashMap<>();
    private static final TestPolicyManager POLICY_MANAGER;
    private static final AtomicBoolean active = new AtomicBoolean(false);

    static {
        PathLookup pathLookup = new TestPathLookup(BASE_DIR_PATHS);
        try {
            if (isEnabledForTests()) {
                POLICY_MANAGER = createPolicyManager(pathLookup);
                loadAgent(POLICY_MANAGER, pathLookup);
            } else {
                POLICY_MANAGER = null;
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static void initialize(Path tempDir) throws IOException {
        if (POLICY_MANAGER != null) {
            var previousTempDir = BASE_DIR_PATHS.put(TEMP, List.of(tempDir));
            assert previousTempDir == null : "Test entitlement bootstrap called multiple times";
        }
    }

    @Override
    public Statement apply(Statement base, Description description) {
        assert description.isSuite() : "must be used as ClassRule";

        // class / suite level
        boolean withoutEntitlements = description.getAnnotation(ESTestCase.WithoutEntitlements.class) != null;
        boolean withEntitlementsOnTestCode = description.getAnnotation(ESTestCase.WithEntitlementsOnTestCode.class) != null;
        var entitledPackages = description.getAnnotation(ESTestCase.EntitledTestPackages.class);

        if (POLICY_MANAGER != null) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    if (active.compareAndSet(false, true)) {
                        try {
                            BASE_DIR_PATHS.keySet().retainAll(List.of(TEMP));
                            POLICY_MANAGER.setActive(false == withoutEntitlements);
                            POLICY_MANAGER.setTriviallyAllowingTestCode(false == withEntitlementsOnTestCode);
                            if (entitledPackages != null) {
                                assert entitledPackages.value().length > 0 : "No test packages specified in @EntitledTestPackages";
                                POLICY_MANAGER.setEntitledTestPackages(entitledPackages.value());
                            } else {
                                POLICY_MANAGER.setEntitledTestPackages();
                            }
                            POLICY_MANAGER.clearModuleEntitlementsCache();
                            // evaluate the suite
                            base.evaluate();
                        } finally {
                            BASE_DIR_PATHS.keySet().retainAll(List.of(TEMP));
                            POLICY_MANAGER.resetAfterTest();
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
        if (POLICY_MANAGER == null) {
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
        BASE_DIR_PATHS.keySet().retainAll(List.of(TEMP));
        POLICY_MANAGER.clearModuleEntitlementsCache();
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
        BASE_DIR_PATHS.compute(BaseDir.CONFIG, baseDirModifier(Collection::add, entitledNodePaths.configDir()));
        BASE_DIR_PATHS.compute(BaseDir.DATA, baseDirModifier(Collection::add, entitledNodePaths.dataDirs()));
        BASE_DIR_PATHS.compute(BaseDir.SHARED_DATA, baseDirModifier(Collection::add, entitledNodePaths.sharedDataDir()));
        BASE_DIR_PATHS.compute(BaseDir.SHARED_REPO, baseDirModifier(Collection::add, entitledNodePaths.repoDirs()));
        POLICY_MANAGER.clearModuleEntitlementsCache();
    }

    private void removeEntitledNodePaths(EntitledNodePaths entitledNodePaths) {
        logger.debug("Removing {}", entitledNodePaths);
        BASE_DIR_PATHS.compute(BaseDir.CONFIG, baseDirModifier(Collection::remove, entitledNodePaths.configDir()));
        BASE_DIR_PATHS.compute(BaseDir.DATA, baseDirModifier(Collection::remove, entitledNodePaths.dataDirs()));
        BASE_DIR_PATHS.compute(BaseDir.SHARED_DATA, baseDirModifier(Collection::remove, entitledNodePaths.sharedDataDir()));
        BASE_DIR_PATHS.compute(BaseDir.SHARED_REPO, baseDirModifier(Collection::remove, entitledNodePaths.repoDirs()));
        POLICY_MANAGER.clearModuleEntitlementsCache();
    }

    // This must allow for duplicate paths between nodes, the config dir for instance is shared across all nodes.
    private static BiFunction<BaseDir, Collection<Path>, Collection<Path>> baseDirModifier(
        BiConsumer<Collection<Path>, Path> operation,
        Path... updates
    ) {
        // always return a new unmodifiable copy
        return (BaseDir baseDir, Collection<Path> paths) -> {
            paths = paths == null ? new ArrayList<>() : new ArrayList<>(paths);
            for (Path update : updates) {
                operation.accept(paths, update);
            }
            return Collections.unmodifiableCollection(paths);
        };
    }

    public static boolean isEnabledForTests() {
        return Booleans.parseBoolean(System.getProperty("es.entitlement.enableForTests", "false"));
    }

    private static TestPolicyManager createPolicyManager(PathLookup pathLookup) throws IOException {
        var pluginsTestBuildInfo = TestBuildInfoParser.parseAllPluginTestBuildInfo();
        var serverTestBuildInfo = TestBuildInfoParser.parseServerTestBuildInfo();
        List<String> pluginNames = pluginsTestBuildInfo.stream().map(TestBuildInfo::component).toList();

        var pluginDescriptors = parsePluginsDescriptors(pluginNames);
        Set<String> modularPlugins = pluginDescriptors.stream()
            .filter(PluginDescriptor::isModular)
            .map(PluginDescriptor::getName)
            .collect(toSet());
        var scopeResolver = TestScopeResolver.createScopeResolver(serverTestBuildInfo, pluginsTestBuildInfo, modularPlugins);
        var pluginsData = pluginDescriptors.stream()
            .map(descriptor -> new TestPluginData(descriptor.getName(), descriptor.isModular(), false))
            .toList();
        Map<String, Policy> pluginPolicies = parsePluginsPolicies(pluginsData);

        String separator = System.getProperty("path.separator");

        // In production, plugins would have access to their respective bundle directories,
        // and so they'd be able to read from their jars. In testing, we approximate this
        // by considering the entire classpath to be "source paths" of all plugins. This
        // also has the effect of granting read access to everything on the test-only classpath,
        // which is fine, because any entitlement errors there could only be false positives.
        String classPathProperty = System.getProperty("java.class.path");

        Set<Path> classPathEntries;
        if (classPathProperty == null) {
            classPathEntries = Set.of();
        } else {
            classPathEntries = Arrays.stream(classPathProperty.split(separator)).map(PathUtils::get).collect(toCollection(TreeSet::new));
        }
        FilesEntitlementsValidation.validate(pluginPolicies, pathLookup);

        String testOnlyPathString = System.getenv("es.entitlement.testOnlyPath");
        Set<URI> testOnlyClassPath;
        if (testOnlyPathString == null) {
            testOnlyClassPath = Set.of();
        } else {
            testOnlyClassPath = Arrays.stream(testOnlyPathString.split(separator))
                .map(PathUtils::get)
                .map(Path::toUri)
                .collect(toCollection(TreeSet::new));
        }

        return new TestPolicyManager(
            HardcodedEntitlements.serverPolicy(null, null),
            HardcodedEntitlements.agentEntitlements(),
            pluginPolicies,
            scopeResolver,
            pathLookup,
            classPathEntries,
            testOnlyClassPath
        );
    }

    private static void loadAgent(PolicyManager policyManager, PathLookup pathLookup) {
        logger.debug("Loading entitlement agent");
        EntitlementInitialization.initializeArgs = new EntitlementInitialization.InitializeArgs(pathLookup, Set.of(), policyManager);
        EntitlementBootstrap.loadAgent(EntitlementBootstrap.findAgentJar(), EntitlementInitialization.class.getName());
    }

    private static Map<String, Policy> parsePluginsPolicies(List<TestPluginData> pluginsData) {
        Map<String, Policy> policies = new HashMap<>();
        for (var pluginData : pluginsData) {
            String pluginName = pluginData.pluginName();
            var resourceName = Strings.format("META-INF/es-plugins/%s/entitlement-policy.yaml", pluginName);

            var resource = EntitlementInitialization.class.getClassLoader().getResource(resourceName);
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
            var resource = EntitlementInitialization.class.getClassLoader().getResource(resourceName);
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

    private record TestPluginData(String pluginName, boolean isModular, boolean isExternalPlugin) {}

}
