/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ReleaseVersions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.network.IfConfig;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap;
import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.PolicyUtils;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.jdk.RuntimeVersionFeature;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.PluginBundle;
import org.elasticsearch.plugins.PluginsLoader;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.bootstrap.BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING;
import static org.elasticsearch.nativeaccess.WindowsFunctions.ConsoleCtrlHandler.CTRL_CLOSE_EVENT;

/**
 * This class starts elasticsearch.
 */
class Elasticsearch {

    private static final String POLICY_PATCH_PREFIX = "es.entitlements.policy.";
    private static final String SERVER_POLICY_PATCH_NAME = POLICY_PATCH_PREFIX + "server";

    /**
     * Main entry point for starting elasticsearch.
     */
    public static void main(final String[] args) {

        Bootstrap bootstrap = initPhase1();
        assert bootstrap != null;

        try {
            initPhase2(bootstrap);
            initPhase3(bootstrap);
        } catch (NodeValidationException e) {
            bootstrap.exitWithNodeValidationException(e);
        } catch (Throwable t) {
            bootstrap.exitWithUnknownException(t);
        }
    }

    @SuppressForbidden(reason = "grab stderr for communication with server-cli")
    private static PrintStream getStderr() {
        return System.err;
    }

    // TODO: remove this, just for debugging
    @SuppressForbidden(reason = "grab stdout for communication with server-cli")
    private static PrintStream getStdout() {
        return System.out;
    }

    /**
     * First phase of process initialization.
     *
     * <p> Phase 1 consists of some static initialization, reading args from the CLI process, and
     * finally initializing logging. As little as possible should be done in this phase because
     * initializing logging is the last step.
     */
    private static Bootstrap initPhase1() {
        final PrintStream out = getStdout();
        final PrintStream err = getStderr();
        final ServerArgs args;

        final boolean useEntitlements = true;
        try {
            initSecurityProperties();

            /*
             * We want the JVM to think there is a security manager installed so that if internal policy decisions that would be based on
             * the presence of a security manager or lack thereof act as if there is a security manager present (e.g., DNS cache policy).
             * This forces such policies to take effect immediately.
             */
            if (useEntitlements == false && RuntimeVersionFeature.isSecurityManagerAvailable()) {
                org.elasticsearch.bootstrap.Security.setSecurityManager(new SecurityManager() {
                    @Override
                    public void checkPermission(Permission perm) {
                        // grant all permissions so that we can later set the security manager to the one that we want
                    }
                });
            }
            LogConfigurator.registerErrorListener();

            BootstrapInfo.init();

            // note that reading server args does *not* close System.in, as it will be read from later for shutdown notification
            var in = new InputStreamStreamInput(System.in);
            args = new ServerArgs(in);

            // mostly just paths are used in phase 1, so secure settings are not needed
            Environment nodeEnv = new Environment(args.nodeSettings(), args.configDir());

            BootstrapInfo.setConsole(ConsoleLoader.loadConsole(nodeEnv));

            // DO NOT MOVE THIS
            // Logging must remain the last step of phase 1. Anything init steps needing logging should be in phase 2.
            LogConfigurator.setNodeName(Node.NODE_NAME_SETTING.get(args.nodeSettings()));
            LogConfigurator.configure(nodeEnv, args.quiet() == false);
        } catch (Throwable t) {
            // any exception this early needs to be fully printed and fail startup
            t.printStackTrace(err);
            err.flush();
            Bootstrap.exit(1); // mimic JDK exit code on exception
            return null; // unreachable, to satisfy compiler
        }

        return new Bootstrap(out, err, args, useEntitlements);
    }

    /**
     * Second phase of process initialization.
     *
     * <p> Phase 2 consists of everything that must occur up to and including security manager initialization.
     */
    private static void initPhase2(Bootstrap bootstrap) throws IOException {
        final ServerArgs args = bootstrap.args();
        final SecureSettings secrets = args.secrets();
        bootstrap.setSecureSettings(secrets);
        Environment nodeEnv = createEnvironment(args.configDir(), args.nodeSettings(), secrets);
        bootstrap.setEnvironment(nodeEnv);

        initPidFile(args.pidFile());

        // install the default uncaught exception handler; must be done before security is
        // initialized as we do not want to grant the runtime permission
        // setDefaultUncaughtExceptionHandler
        Thread.setDefaultUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler());

        bootstrap.spawner().spawnNativeControllers(nodeEnv);

        nodeEnv.validateNativesConfig(); // temporary directories are important for JNA
        initializeNatives(
            nodeEnv.tmpDir(),
            BootstrapSettings.MEMORY_LOCK_SETTING.get(args.nodeSettings()),
            true, // always install system call filters, not user-configurable since 8.0.0
            BootstrapSettings.CTRLHANDLER_SETTING.get(args.nodeSettings())
        );

        // initialize probes before the security manager is installed
        initializeProbes();

        Runtime.getRuntime().addShutdownHook(new Thread(Elasticsearch::shutdown, "elasticsearch-shutdown"));

        // look for jar hell
        final Logger logger = LogManager.getLogger(JarHell.class);
        JarHell.checkJarHell(logger::debug);

        // Log ifconfig output before SecurityManager is installed
        IfConfig.logIfNecessary();

        ensureInitialized(
            // ReleaseVersions does nontrivial static initialization which should always succeed but load it now (before SM) to be sure
            ReleaseVersions.class,
            // ReferenceDocs class does nontrivial static initialization which should always succeed but load it now (before SM) to be sure
            ReferenceDocs.class,
            // The following classes use MethodHandles.lookup during initialization, load them now (before SM) to be sure they succeed
            AbstractRefCounted.class,
            SubscribableListener.class,
            RunOnce.class,
            // We eagerly initialize to work around log4j permissions & JDK-8309727
            VectorUtil.class
        );

        // load the plugin Java modules and layers now for use in entitlements
        var modulesBundles = PluginsLoader.loadModulesBundles(nodeEnv.modulesDir());
        var pluginsBundles = PluginsLoader.loadPluginsBundles(nodeEnv.pluginsDir());

        final PluginsLoader pluginsLoader;

        if (bootstrap.useEntitlements()) {
            LogManager.getLogger(Elasticsearch.class).info("Bootstrapping Entitlements");

            var pluginData = Stream.concat(
                modulesBundles.stream()
                    .map(bundle -> new PolicyUtils.PluginData(bundle.getDir(), bundle.pluginDescriptor().isModular(), false)),
                pluginsBundles.stream()
                    .map(bundle -> new PolicyUtils.PluginData(bundle.getDir(), bundle.pluginDescriptor().isModular(), true))
            ).toList();

            var pluginPolicyPatches = collectPluginPolicyPatches(modulesBundles, pluginsBundles, logger);
            var pluginPolicies = PolicyUtils.createPluginPolicies(pluginData, pluginPolicyPatches, Build.current().version());
            var serverPolicyPatch = PolicyUtils.parseEncodedPolicyIfExists(
                System.getProperty(SERVER_POLICY_PATCH_NAME),
                Build.current().version(),
                false,
                "server",
                PolicyManager.SERVER_LAYER_MODULES.stream().map(Module::getName).collect(Collectors.toUnmodifiableSet())
            );

            pluginsLoader = PluginsLoader.createPluginsLoader(modulesBundles, pluginsBundles, findPluginsWithNativeAccess(pluginPolicies));

            var pluginsResolver = PluginsResolver.create(pluginsLoader);
            Map<String, Path> sourcePaths = Stream.concat(modulesBundles.stream(), pluginsBundles.stream())
                .collect(Collectors.toUnmodifiableMap(bundle -> bundle.pluginDescriptor().getName(), PluginBundle::getDir));
            EntitlementBootstrap.bootstrap(
                serverPolicyPatch,
                pluginPolicies,
                pluginsResolver::resolveClassToPluginName,
                nodeEnv.settings()::getValues,
                nodeEnv.dataDirs(),
                nodeEnv.repoDirs(),
                nodeEnv.configDir(),
                nodeEnv.libDir(),
                nodeEnv.modulesDir(),
                nodeEnv.pluginsDir(),
                sourcePaths,
                nodeEnv.logsDir(),
                nodeEnv.tmpDir(),
                args.pidFile(),
                Set.of(EntitlementSelfTester.class)
            );
            EntitlementSelfTester.entitlementSelfTest();
        } else {
            assert RuntimeVersionFeature.isSecurityManagerAvailable();
            // no need to explicitly enable native access for legacy code
            pluginsLoader = PluginsLoader.createPluginsLoader(modulesBundles, pluginsBundles, Map.of());
            // install SM after natives, shutdown hooks, etc.
            LogManager.getLogger(Elasticsearch.class).info("Bootstrapping java SecurityManager");
            org.elasticsearch.bootstrap.Security.configure(
                nodeEnv,
                SECURITY_FILTER_BAD_DEFAULTS_SETTING.get(args.nodeSettings()),
                args.pidFile()
            );
        }

        bootstrap.setPluginsLoader(pluginsLoader);
    }

    private static Map<String, String> collectPluginPolicyPatches(
        Set<PluginBundle> modulesBundles,
        Set<PluginBundle> pluginsBundles,
        Logger logger
    ) {
        var policyPatches = new HashMap<String, String>();
        var systemProperties = BootstrapInfo.getSystemProperties();
        systemProperties.keys().asIterator().forEachRemaining(key -> {
            var value = systemProperties.get(key);
            if (key instanceof String k
                && value instanceof String v
                && k.startsWith(POLICY_PATCH_PREFIX)
                && k.equals(SERVER_POLICY_PATCH_NAME) == false) {
                policyPatches.put(k.substring(POLICY_PATCH_PREFIX.length()), v);
            }
        });
        var pluginNames = Stream.concat(modulesBundles.stream(), pluginsBundles.stream())
            .map(bundle -> bundle.pluginDescriptor().getName())
            .collect(Collectors.toUnmodifiableSet());

        for (var patchedPluginName : policyPatches.keySet()) {
            if (pluginNames.contains(patchedPluginName) == false) {
                logger.warn(
                    "Found command-line policy patch for unknown plugin [{}] (available plugins: [{}])",
                    patchedPluginName,
                    String.join(", ", pluginNames)
                );
            }
        }
        return policyPatches;
    }

    private static class EntitlementSelfTester {
        // check entitlements were loaded correctly. note this must be outside the entitlements lib.
        private static void entitlementSelfTest() {
            ensureCannotStartProcess(ProcessBuilder::start);
            // Try again with reflection
            ensureCannotStartProcess(EntitlementSelfTester::reflectiveStartProcess);
        }

        private static void ensureCannotStartProcess(CheckedConsumer<ProcessBuilder, ?> startProcess) {
            try {
                // The command doesn't matter; it doesn't even need to exist
                startProcess.accept(new ProcessBuilder(""));
            } catch (NotEntitledException e) {
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
    }

    private static void ensureInitialized(Class<?>... classes) {
        for (final var clazz : classes) {
            try {
                MethodHandles.publicLookup().ensureInitialized(clazz);
            } catch (IllegalAccessException unexpected) {
                throw new AssertionError(unexpected);
            }
        }
    }

    /**
     * Third phase of initialization.
     *
     * <p> Phase 3 consists of everything after security manager is initialized. Up until now, the system has been single
     * threaded. This phase can spawn threads, write to the log, and is subject ot the security manager policy.
     *
     * <p> At the end of phase 3 the system is ready to accept requests and the main thread is ready to terminate. This means:
     * <ul>
     *     <li>The node components have been constructed and started</li>
     *     <li>Cleanup has been done (eg secure settings are closed)</li>
     *     <li>At least one thread other than the main thread is alive and will stay alive after the main thread terminates</li>
     *     <li>The parent CLI process has been notified the system is ready</li>
     * </ul>
     *
     * @param bootstrap the bootstrap state
     * @throws IOException if a problem with filesystem or network occurs
     * @throws NodeValidationException if the node cannot start due to a node configuration issue
     */
    private static void initPhase3(Bootstrap bootstrap) throws IOException, NodeValidationException {
        checkLucene();

        Node node = new Node(bootstrap.environment(), bootstrap.pluginsLoader()) {
            @Override
            protected void validateNodeBeforeAcceptingRequests(
                final BootstrapContext context,
                final BoundTransportAddress boundTransportAddress,
                List<BootstrapCheck> checks
            ) throws NodeValidationException {
                var additionalChecks = new ArrayList<>(checks);
                if (bootstrap.useEntitlements() == false) {
                    additionalChecks.add(new BootstrapChecks.AllPermissionCheck());
                }
                BootstrapChecks.check(context, boundTransportAddress, additionalChecks);
            }
        };
        INSTANCE = new Elasticsearch(bootstrap.spawner(), node);

        // any secure settings must be read during node construction
        IOUtils.close(bootstrap.secureSettings());

        INSTANCE.start();

        if (bootstrap.args().daemonize()) {
            LogConfigurator.removeConsoleAppender();
        }

        // DO NOT MOVE THIS
        // Signaling readiness to accept requests must remain the last step of initialization. Note that it is extremely
        // important closing the err stream to the CLI when daemonizing is the last statement since that is the only
        // way to pass errors to the CLI
        bootstrap.sendCliMarker(BootstrapInfo.SERVER_READY_MARKER);
        if (bootstrap.args().daemonize()) {
            bootstrap.closeStreams();
        } else {
            startCliMonitorThread(System.in);
        }
    }

    /**
     * Initialize native resources.
     *
     * @param tmpFile          the temp directory
     * @param mlockAll         whether or not to lock memory
     * @param systemCallFilter whether or not to install system call filters
     * @param ctrlHandler      whether or not to install the ctrl-c handler (applies to Windows only)
     */
    static void initializeNatives(final Path tmpFile, final boolean mlockAll, final boolean systemCallFilter, final boolean ctrlHandler) {
        final Logger logger = LogManager.getLogger(Elasticsearch.class);
        var nativeAccess = NativeAccess.instance();

        // check if the user is running as root, and bail
        if (nativeAccess.definitelyRunningAsRoot()) {
            throw new RuntimeException("can not run elasticsearch as root");
        }

        if (systemCallFilter) {
            /*
             * Try to install system call filters; if they fail to install; a bootstrap check will fail startup in production mode.
             *
             * TODO: should we fail hard here if system call filters fail to install, or remain lenient in non-production environments?
             */
            nativeAccess.tryInstallExecSandbox();
        }

        // mlockall if requested
        if (mlockAll) {
            nativeAccess.tryLockMemory();
        }

        // listener for windows close event
        if (ctrlHandler) {
            var windowsFunctions = nativeAccess.getWindowsFunctions();
            if (windowsFunctions != null) {
                windowsFunctions.addConsoleCtrlHandler(code -> {
                    if (CTRL_CLOSE_EVENT == code) {
                        logger.info("running graceful exit on windows");
                        shutdown();
                        return true;
                    }
                    return false;
                });
            }
        }

        // init lucene random seed. it will use /dev/urandom where available:
        StringHelper.randomId();
    }

    static void initializeProbes() {
        // Force probes to be loaded
        ProcessProbe.getInstance();
        OsProbe.getInstance();
        JvmInfo.jvmInfo();
        HotThreads.initializeRuntimeMonitoring();
    }

    static void checkLucene() {
        if (IndexVersion.current().luceneVersion().equals(org.apache.lucene.util.Version.LATEST) == false) {
            throw new AssertionError(
                "Lucene version mismatch this version of Elasticsearch requires lucene version ["
                    + IndexVersion.current().luceneVersion()
                    + "]  but the current lucene version is ["
                    + org.apache.lucene.util.Version.LATEST
                    + "]"
            );
        }
    }

    /**
     * Starts a thread that monitors stdin for a shutdown signal.
     *
     * If the shutdown signal is received, Elasticsearch exits with status code 0.
     * If the pipe is broken, Elasticsearch exits with status code 1.
     *
     * @param stdin Standard input for this process
     */
    private static void startCliMonitorThread(InputStream stdin) {
        new Thread(() -> {
            int msg = -1;
            try {
                msg = stdin.read();
            } catch (IOException e) {
                // ignore, whether we cleanly got end of stream (-1) or an error, we will shut down below
            } finally {
                if (msg == BootstrapInfo.SERVER_SHUTDOWN_MARKER) {
                    Bootstrap.exit(0);
                } else {
                    // parent process died or there was an error reading from it
                    Bootstrap.exit(1);
                }
            }
        }, "elasticsearch-cli-monitor-thread").start();
    }

    /**
     * Writes the current process id into the given pidfile, if not null. The pidfile is cleaned up on system exit.
     *
     * @param pidFile A path to a file, or null of no pidfile should be written
     */
    private static void initPidFile(Path pidFile) throws IOException {
        if (pidFile == null) {
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.deleteIfExists(pidFile);
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to delete pid file " + pidFile, e);
            }
        }, "elasticsearch[pidfile-cleanup]"));

        // It has to be an absolute path, otherwise pidFile.getParent() will return null
        assert pidFile.isAbsolute();

        if (Files.exists(pidFile.getParent()) == false) {
            Files.createDirectories(pidFile.getParent());
        }

        Files.writeString(pidFile, Long.toString(ProcessHandle.current().pid()));
    }

    private static void initSecurityProperties() {
        for (final String property : new String[] { "networkaddress.cache.ttl", "networkaddress.cache.negative.ttl" }) {
            final String overrideProperty = "es." + property;
            final String overrideValue = System.getProperty(overrideProperty);
            if (overrideValue != null) {
                try {
                    // round-trip the property to an integer and back to a string to ensure that it parses properly
                    Security.setProperty(property, Integer.toString(Integer.valueOf(overrideValue)));
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("failed to parse [" + overrideProperty + "] with value [" + overrideValue + "]", e);
                }
            }
        }

        // policy file codebase declarations in security.policy rely on property expansion, see PolicyUtil.readPolicy
        Security.setProperty("policy.expandProperties", "true");
    }

    private static Environment createEnvironment(Path configDir, Settings initialSettings, SecureSettings secureSettings) {
        Settings.Builder builder = Settings.builder();
        builder.put(initialSettings);
        if (secureSettings != null) {
            builder.setSecureSettings(secureSettings);
        }
        return new Environment(builder.build(), configDir);
    }

    static Map<String, Set<String>> findPluginsWithNativeAccess(Map<String, Policy> policies) {
        Map<String, Set<String>> pluginsWithNativeAccess = new HashMap<>();
        for (var kv : policies.entrySet()) {
            for (var scope : kv.getValue().scopes()) {
                if (scope.entitlements().stream().anyMatch(entitlement -> entitlement instanceof LoadNativeLibrariesEntitlement)) {
                    var modulesToEnable = pluginsWithNativeAccess.computeIfAbsent(kv.getKey(), k -> new HashSet<>());
                    modulesToEnable.add(scope.moduleName());
                }
            }
        }
        return pluginsWithNativeAccess;
    }

    // -- instance

    private static volatile Elasticsearch INSTANCE;

    private final Spawner spawner;
    private final Node node;
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
    private final Thread keepAliveThread;

    private Elasticsearch(Spawner spawner, Node node) {
        this.spawner = Objects.requireNonNull(spawner);
        this.node = Objects.requireNonNull(node);
        this.keepAliveThread = new Thread(() -> {
            try {
                keepAliveLatch.await();
            } catch (InterruptedException e) {
                // bail out
            }
        }, "elasticsearch[keepAlive/" + Build.current().version() + "]");
    }

    private void start() throws NodeValidationException {
        node.start();
        keepAliveThread.start();
    }

    private static void shutdown() {
        ElasticsearchProcess.markStopping();

        if (INSTANCE == null) {
            return; // never got far enough
        }
        var es = INSTANCE;
        try {
            es.node.prepareForClose();
            IOUtils.close(es.node, es.spawner);
            if (es.node.awaitClose(10, TimeUnit.SECONDS) == false) {
                throw new IllegalStateException(
                    "Node didn't stop within 10 seconds. " + "Any outstanding requests or tasks might get killed."
                );
            }
        } catch (IOException ex) {
            throw new ElasticsearchException("Failure occurred while shutting down node", ex);
        } catch (InterruptedException e) {
            LogManager.getLogger(Elasticsearch.class).warn("Thread got interrupted while waiting for the node to shutdown.");
            Thread.currentThread().interrupt();
        } finally {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configurator.shutdown(context);

            es.keepAliveLatch.countDown();
        }
    }
}
