/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bootstrap;

import org.elasticsearch.bootstrap.TestBuildInfo;
import org.elasticsearch.bootstrap.TestBuildInfoParser;
import org.elasticsearch.bootstrap.TestScopeResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.initialization.EntitlementInitialization;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyParser;
import org.elasticsearch.entitlement.runtime.policy.TestPathLookup;
import org.elasticsearch.entitlement.runtime.policy.TestPolicyManager;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.PluginDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.TEMP;
import static org.elasticsearch.env.Environment.PATH_DATA_SETTING;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.env.Environment.PATH_REPO_SETTING;
import static org.elasticsearch.env.Environment.PATH_SHARED_DATA_SETTING;

public class TestEntitlementBootstrap {

    private static final Logger logger = LogManager.getLogger(TestEntitlementBootstrap.class);

    private static Map<BaseDir, Collection<Path>> baseDirPaths = new ConcurrentHashMap<>();
    private static TestPolicyManager policyManager;

    /**
     * Activates entitlement checking in tests.
     */
    public static void bootstrap(@Nullable Path tempDir) throws IOException {
        if (isEnabledForTest() == false) {
            return;
        }
        var previousTempDir = baseDirPaths.put(TEMP, zeroOrOne(tempDir));
        assert previousTempDir == null : "Test entitlement bootstrap called multiple times";
        TestPathLookup pathLookup = new TestPathLookup(baseDirPaths);
        policyManager = createPolicyManager(pathLookup);
        EntitlementInitialization.initializeArgs = new EntitlementInitialization.InitializeArgs(pathLookup, Set.of(), policyManager);
        logger.debug("Loading entitlement agent");
        EntitlementBootstrap.loadAgent(EntitlementBootstrap.findAgentJar(), EntitlementInitialization.class.getName());
    }

    public static void registerNodeBaseDirs(Settings settings, Path configPath) {
        if (policyManager == null) {
            return;
        }
        Path homeDir = absolutePath(PATH_HOME_SETTING.get(settings));
        Path configDir = configPath != null ? configPath : homeDir.resolve("config");
        Collection<Path> dataDirs = dataDirs(settings, homeDir);
        Collection<Path> sharedDataDir = sharedDataDir(settings);
        Collection<Path> repoDirs = repoDirs(settings);
        logger.debug("Registering node dirs: config [{}], dataDirs [{}], repoDirs [{}]", configDir, dataDirs, repoDirs);
        baseDirPaths.compute(BaseDir.CONFIG, baseDirModifier(paths -> paths.add(configDir)));
        baseDirPaths.compute(BaseDir.DATA, baseDirModifier(paths -> paths.addAll(dataDirs)));
        baseDirPaths.compute(BaseDir.SHARED_DATA, baseDirModifier(paths -> paths.addAll(sharedDataDir)));
        baseDirPaths.compute(BaseDir.SHARED_REPO, baseDirModifier(paths -> paths.addAll(repoDirs)));
        policyManager.reset();
    }

    public static void unregisterNodeBaseDirs(Settings settings, Path configPath) {
        if (policyManager == null) {
            return;
        }
        Path homeDir = absolutePath(PATH_HOME_SETTING.get(settings));
        Path configDir = configPath != null ? configPath : homeDir.resolve("config");
        Collection<Path> dataDirs = dataDirs(settings, homeDir);
        Collection<Path> sharedDataDir = sharedDataDir(settings);
        Collection<Path> repoDirs = repoDirs(settings);
        logger.debug("Unregistering node dirs: config [{}], dataDirs [{}], repoDirs [{}]", configDir, dataDirs, repoDirs);
        baseDirPaths.compute(BaseDir.CONFIG, baseDirModifier(paths -> paths.remove(configDir)));
        baseDirPaths.compute(BaseDir.DATA, baseDirModifier(paths -> paths.removeAll(dataDirs)));
        baseDirPaths.compute(BaseDir.SHARED_DATA, baseDirModifier(paths -> paths.removeAll(sharedDataDir)));
        baseDirPaths.compute(BaseDir.SHARED_REPO, baseDirModifier(paths -> paths.removeAll(repoDirs)));
        policyManager.reset();
    }

    private static Collection<Path> dataDirs(Settings settings, Path homeDir) {
        List<String> dataDirs = PATH_DATA_SETTING.get(settings);
        return dataDirs.isEmpty()
            ? List.of(homeDir.resolve("data"))
            : dataDirs.stream().map(TestEntitlementBootstrap::absolutePath).toList();
    }

    private static Collection<Path> sharedDataDir(Settings settings) {
        String sharedDataDir = PATH_SHARED_DATA_SETTING.get(settings);
        return Strings.hasText(sharedDataDir) ? List.of(absolutePath(sharedDataDir)) : List.of();
    }

    private static Collection<Path> repoDirs(Settings settings) {
        return PATH_REPO_SETTING.get(settings).stream().map(TestEntitlementBootstrap::absolutePath).toList();
    }

    private static BiFunction<BaseDir, Collection<Path>, Collection<Path>> baseDirModifier(Consumer<Collection<Path>> consumer) {
        return (BaseDir baseDir, Collection<Path> paths) -> {
            if (paths == null) {
                paths = new HashSet<>();
            }
            consumer.accept(paths);
            return paths;
        };
    }

    @SuppressForbidden(reason = "must be resolved using the default file system, rather then the mocked test file system")
    private static Path absolutePath(String path) {
        return Paths.get(path).toAbsolutePath().normalize();
    }

    private static <T> List<T> zeroOrOne(T item) {
        if (item == null) {
            return List.of();
        } else {
            return List.of(item);
        }
    }

    public static boolean isEnabledForTest() {
        return Booleans.parseBoolean(System.getProperty("es.entitlement.enableForTests", "false"));
    }

    public static void setActive(boolean newValue) {
        policyManager.setActive(newValue);
    }

    public static void setTriviallyAllowingTestCode(boolean newValue) {
        policyManager.setTriviallyAllowingTestCode(newValue);
    }

    public static void setEntitledTestPackages(String[] entitledTestPackages) {
        policyManager.setEntitledTestPackages(entitledTestPackages);
    }

    public static void reset() {
        if (policyManager != null) {
            policyManager.reset();
        }
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

        // In productions, plugins would have access to their respective bundle directories,
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
