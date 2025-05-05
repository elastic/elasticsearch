/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Strings;
import org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.FileAccessTree;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.PolicyUtils;
import org.elasticsearch.entitlement.runtime.policy.Scope;
import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.FileData;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ManageThreadsEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ReadStoreAttributesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.SetHttpsConnectionPropertiesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteSystemPropertiesEntitlement;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.CONFIG;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.DATA;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.LIB;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.LOGS;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.MODULES;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.PLUGINS;
import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.SHARED_REPO;
import static org.elasticsearch.entitlement.runtime.policy.Platform.LINUX;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ_WRITE;

/**
 * Called by the agent during {@code agentmain} to configure the entitlement system,
 * instantiate and configure an {@link EntitlementChecker},
 * make it available to the bootstrap library via {@link #checker()},
 * and then install the {@link org.elasticsearch.entitlement.instrumentation.Instrumenter}
 * to begin injecting our instrumentation.
 */
public class EntitlementInitialization {

    private static final Module ENTITLEMENTS_MODULE = PolicyManager.class.getModule();

    private static ElasticsearchEntitlementChecker manager;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return manager;
    }

    /**
     * Initializes the Entitlement system:
     * <ol>
     * <li>
     * Initialize dynamic instrumentation via {@link DynamicInstrumentation#initialize}
     * </li>
     * <li>
     * Creates the {@link PolicyManager}
     * </li>
     * <li>
     * Creates the {@link ElasticsearchEntitlementChecker} instance referenced by the instrumented methods
     * </li>
     * </ol>
     * <p>
     * <strong>NOTE:</strong> this method is referenced by the agent reflectively
     * </p>
     *
     * @param inst the JVM instrumentation class instance
     */
    public static void initialize(Instrumentation inst) throws Exception {
        manager = initChecker();

        var verifyBytecode = Booleans.parseBoolean(System.getProperty("es.entitlements.verify_bytecode", "false"));
        if (verifyBytecode) {
            ensureClassesSensitiveToVerificationAreInitialized();
        }

        DynamicInstrumentation.initialize(inst, getVersionSpecificCheckerClass(EntitlementChecker.class), verifyBytecode);
    }

    private static PolicyManager createPolicyManager() {
        EntitlementBootstrap.BootstrapArgs bootstrapArgs = EntitlementBootstrap.bootstrapArgs();
        Map<String, Policy> pluginPolicies = bootstrapArgs.pluginPolicies();
        PathLookup pathLookup = bootstrapArgs.pathLookup();

        List<Scope> serverScopes = new ArrayList<>();
        List<FileData> serverModuleFileDatas = new ArrayList<>();
        Collections.addAll(
            serverModuleFileDatas,
            // Base ES directories
            FileData.ofBaseDirPath(PLUGINS, READ),
            FileData.ofBaseDirPath(MODULES, READ),
            FileData.ofBaseDirPath(CONFIG, READ),
            FileData.ofBaseDirPath(LOGS, READ_WRITE),
            FileData.ofBaseDirPath(LIB, READ),
            FileData.ofBaseDirPath(DATA, READ_WRITE),
            FileData.ofBaseDirPath(SHARED_REPO, READ_WRITE),
            // exclusive settings file
            FileData.ofRelativePath(Path.of("operator/settings.json"), CONFIG, READ_WRITE).withExclusive(true),

            // OS release on Linux
            FileData.ofPath(Path.of("/etc/os-release"), READ).withPlatform(LINUX),
            FileData.ofPath(Path.of("/etc/system-release"), READ).withPlatform(LINUX),
            FileData.ofPath(Path.of("/usr/lib/os-release"), READ).withPlatform(LINUX),
            // read max virtual memory areas
            FileData.ofPath(Path.of("/proc/sys/vm/max_map_count"), READ).withPlatform(LINUX),
            FileData.ofPath(Path.of("/proc/meminfo"), READ).withPlatform(LINUX),
            // load averages on Linux
            FileData.ofPath(Path.of("/proc/loadavg"), READ).withPlatform(LINUX),
            // control group stats on Linux. cgroup v2 stats are in an unpredicable
            // location under `/sys/fs/cgroup`, so unfortunately we have to allow
            // read access to the entire directory hierarchy.
            FileData.ofPath(Path.of("/proc/self/cgroup"), READ).withPlatform(LINUX),
            FileData.ofPath(Path.of("/sys/fs/cgroup/"), READ).withPlatform(LINUX),
            // // io stats on Linux
            FileData.ofPath(Path.of("/proc/self/mountinfo"), READ).withPlatform(LINUX),
            FileData.ofPath(Path.of("/proc/diskstats"), READ).withPlatform(LINUX)
        );
        if (pathLookup.pidFile() != null) {
            serverModuleFileDatas.add(FileData.ofPath(pathLookup.pidFile(), READ_WRITE));
        }

        Collections.addAll(
            serverScopes,
            new Scope(
                "org.elasticsearch.base",
                List.of(
                    new CreateClassLoaderEntitlement(),
                    new FilesEntitlement(
                        List.of(
                            // TODO: what in es.base is accessing shared repo?
                            FileData.ofBaseDirPath(SHARED_REPO, READ_WRITE),
                            FileData.ofBaseDirPath(DATA, READ_WRITE)
                        )
                    )
                )
            ),
            new Scope("org.elasticsearch.xcontent", List.of(new CreateClassLoaderEntitlement())),
            new Scope(
                "org.elasticsearch.server",
                List.of(
                    new ExitVMEntitlement(),
                    new ReadStoreAttributesEntitlement(),
                    new CreateClassLoaderEntitlement(),
                    new InboundNetworkEntitlement(),
                    new LoadNativeLibrariesEntitlement(),
                    new ManageThreadsEntitlement(),
                    new FilesEntitlement(serverModuleFileDatas)
                )
            ),
            new Scope("java.desktop", List.of(new LoadNativeLibrariesEntitlement())),
            new Scope("org.apache.httpcomponents.httpclient", List.of(new OutboundNetworkEntitlement())),
            new Scope(
                "org.apache.lucene.core",
                List.of(
                    new LoadNativeLibrariesEntitlement(),
                    new ManageThreadsEntitlement(),
                    new FilesEntitlement(List.of(FileData.ofBaseDirPath(CONFIG, READ), FileData.ofBaseDirPath(DATA, READ_WRITE)))
                )
            ),
            new Scope("org.apache.lucene.misc", List.of(new FilesEntitlement(List.of(FileData.ofBaseDirPath(DATA, READ_WRITE))))),
            new Scope(
                "org.apache.logging.log4j.core",
                List.of(new ManageThreadsEntitlement(), new FilesEntitlement(List.of(FileData.ofBaseDirPath(LOGS, READ_WRITE))))
            ),
            new Scope(
                "org.elasticsearch.nativeaccess",
                List.of(new LoadNativeLibrariesEntitlement(), new FilesEntitlement(List.of(FileData.ofBaseDirPath(DATA, READ_WRITE))))
            )
        );

        // conditionally add FIPS entitlements if FIPS only functionality is enforced
        if (Booleans.parseBoolean(System.getProperty("org.bouncycastle.fips.approved_only"), false)) {
            // if custom trust store is set, grant read access to its location, otherwise use the default JDK trust store
            String trustStore = System.getProperty("javax.net.ssl.trustStore");
            Path trustStorePath = trustStore != null
                ? Path.of(trustStore)
                : Path.of(System.getProperty("java.home")).resolve("lib/security/jssecacerts");

            Collections.addAll(
                serverScopes,
                new Scope(
                    "org.bouncycastle.fips.tls",
                    List.of(
                        new FilesEntitlement(List.of(FileData.ofPath(trustStorePath, READ))),
                        new ManageThreadsEntitlement(),
                        new OutboundNetworkEntitlement()
                    )
                ),
                new Scope(
                    "org.bouncycastle.fips.core",
                    // read to lib dir is required for checksum validation
                    List.of(new FilesEntitlement(List.of(FileData.ofBaseDirPath(LIB, READ))), new ManageThreadsEntitlement())
                )
            );
        }

        var serverPolicy = new Policy(
            "server",
            bootstrapArgs.serverPolicyPatch() == null
                ? serverScopes
                : PolicyUtils.mergeScopes(serverScopes, bootstrapArgs.serverPolicyPatch().scopes())
        );

        // agents run without a module, so this is a special hack for the apm agent
        // this should be removed once https://github.com/elastic/elasticsearch/issues/109335 is completed
        // See also modules/apm/src/main/plugin-metadata/entitlement-policy.yaml
        List<Entitlement> agentEntitlements = List.of(
            new CreateClassLoaderEntitlement(),
            new ManageThreadsEntitlement(),
            new SetHttpsConnectionPropertiesEntitlement(),
            new OutboundNetworkEntitlement(),
            new WriteSystemPropertiesEntitlement(Set.of("AsyncProfiler.safemode")),
            new LoadNativeLibrariesEntitlement(),
            new FilesEntitlement(
                List.of(
                    FileData.ofBaseDirPath(LOGS, READ_WRITE),
                    FileData.ofPath(Path.of("/proc/meminfo"), READ),
                    FileData.ofPath(Path.of("/sys/fs/cgroup/"), READ)
                )
            )
        );

        validateFilesEntitlements(pluginPolicies, pathLookup);

        return new PolicyManager(
            serverPolicy,
            agentEntitlements,
            pluginPolicies,
            EntitlementBootstrap.bootstrapArgs().scopeResolver(),
            EntitlementBootstrap.bootstrapArgs().sourcePaths(),
            ENTITLEMENTS_MODULE,
            pathLookup,
            bootstrapArgs.suppressFailureLogClasses()
        );
    }

    // package visible for tests
    static void validateFilesEntitlements(Map<String, Policy> pluginPolicies, PathLookup pathLookup) {
        Set<Path> readAccessForbidden = new HashSet<>();
        pathLookup.getBaseDirPaths(PLUGINS).forEach(p -> readAccessForbidden.add(p.toAbsolutePath().normalize()));
        pathLookup.getBaseDirPaths(MODULES).forEach(p -> readAccessForbidden.add(p.toAbsolutePath().normalize()));
        pathLookup.getBaseDirPaths(LIB).forEach(p -> readAccessForbidden.add(p.toAbsolutePath().normalize()));
        Set<Path> writeAccessForbidden = new HashSet<>();
        pathLookup.getBaseDirPaths(CONFIG).forEach(p -> writeAccessForbidden.add(p.toAbsolutePath().normalize()));
        for (var pluginPolicy : pluginPolicies.entrySet()) {
            for (var scope : pluginPolicy.getValue().scopes()) {
                var filesEntitlement = scope.entitlements()
                    .stream()
                    .filter(x -> x instanceof FilesEntitlement)
                    .map(x -> ((FilesEntitlement) x))
                    .findFirst();
                if (filesEntitlement.isPresent()) {
                    var fileAccessTree = FileAccessTree.withoutExclusivePaths(filesEntitlement.get(), pathLookup, null);
                    validateReadFilesEntitlements(pluginPolicy.getKey(), scope.moduleName(), fileAccessTree, readAccessForbidden);
                    validateWriteFilesEntitlements(pluginPolicy.getKey(), scope.moduleName(), fileAccessTree, writeAccessForbidden);
                }
            }
        }
    }

    private static IllegalArgumentException buildValidationException(
        String componentName,
        String moduleName,
        Path forbiddenPath,
        FilesEntitlement.Mode mode
    ) {
        return new IllegalArgumentException(
            Strings.format(
                "policy for module [%s] in [%s] has an invalid file entitlement. Any path under [%s] is forbidden for mode [%s].",
                moduleName,
                componentName,
                forbiddenPath,
                mode
            )
        );
    }

    private static void validateReadFilesEntitlements(
        String componentName,
        String moduleName,
        FileAccessTree fileAccessTree,
        Set<Path> readForbiddenPaths
    ) {

        for (Path forbiddenPath : readForbiddenPaths) {
            if (fileAccessTree.canRead(forbiddenPath)) {
                throw buildValidationException(componentName, moduleName, forbiddenPath, READ);
            }
        }
    }

    private static void validateWriteFilesEntitlements(
        String componentName,
        String moduleName,
        FileAccessTree fileAccessTree,
        Set<Path> writeForbiddenPaths
    ) {
        for (Path forbiddenPath : writeForbiddenPaths) {
            if (fileAccessTree.canWrite(forbiddenPath)) {
                throw buildValidationException(componentName, moduleName, forbiddenPath, READ_WRITE);
            }
        }
    }

    /**
     * If bytecode verification is enabled, ensure these classes get loaded before transforming/retransforming them.
     * For these classes, the order in which we transform and verify them matters. Verification during class transformation is at least an
     * unforeseen (if not unsupported) scenario: we are loading a class, and while we are still loading it (during transformation) we try
     * to verify it. This in turn leads to more classes loading (for verification purposes), which could turn into those classes to be
     * transformed and undergo verification. In order to avoid circularity errors as much as possible, we force a partial order.
     */
    private static void ensureClassesSensitiveToVerificationAreInitialized() {
        var classesToInitialize = Set.of("sun.net.www.protocol.http.HttpURLConnection");
        for (String className : classesToInitialize) {
            try {
                Class.forName(className);
            } catch (ClassNotFoundException unexpected) {
                throw new AssertionError(unexpected);
            }
        }
    }

    /**
     * Returns the "most recent" checker class compatible with the current runtime Java version.
     * For checkers, we have (optionally) version specific classes, each with a prefix (e.g. Java23).
     * The mapping cannot be automatic, as it depends on the actual presence of these classes in the final Jar (see
     * the various mainXX source sets).
     */
    private static Class<?> getVersionSpecificCheckerClass(Class<?> baseClass) {
        String packageName = baseClass.getPackageName();
        String baseClassName = baseClass.getSimpleName();
        int javaVersion = Runtime.version().feature();

        final String classNamePrefix;
        if (javaVersion >= 23) {
            // All Java version from 23 onwards will be able to use che checks in the Java23EntitlementChecker interface and implementation
            classNamePrefix = "Java23";
        } else {
            // For any other Java version, the basic EntitlementChecker interface and implementation contains all the supported checks
            classNamePrefix = "";
        }
        final String className = packageName + "." + classNamePrefix + baseClassName;
        Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("entitlement lib cannot find entitlement class " + className, e);
        }
        return clazz;
    }

    private static ElasticsearchEntitlementChecker initChecker() {
        final PolicyManager policyManager = createPolicyManager();

        final Class<?> clazz = getVersionSpecificCheckerClass(ElasticsearchEntitlementChecker.class);

        Constructor<?> constructor;
        try {
            constructor = clazz.getConstructor(PolicyManager.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError("entitlement impl is missing no arg constructor", e);
        }
        try {
            return (ElasticsearchEntitlementChecker) constructor.newInstance(policyManager);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new AssertionError(e);
        }
    }
}
