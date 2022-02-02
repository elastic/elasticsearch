/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.plugins.PluginsManager;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.PidFile;
import org.elasticsearch.common.filesystem.FileSystemNatives;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.IfConfig;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Internal startup code.
 */
final class Bootstrap {

    private static volatile Bootstrap INSTANCE;
    private volatile Node node;
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
    private final Thread keepAliveThread;
    private final Spawner spawner = new Spawner();

    /** creates a new instance */
    Bootstrap() {
        keepAliveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    keepAliveLatch.await();
                } catch (InterruptedException e) {
                    // bail out
                }
            }
        }, "elasticsearch[keepAlive/" + Version.CURRENT + "]");
        keepAliveThread.setDaemon(false);
        // keep this thread alive (non daemon thread) until we shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                keepAliveLatch.countDown();
            }
        });
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
        final Logger logger = LogManager.getLogger(Bootstrap.class);

        // check if the user is running as root, and bail
        if (Natives.definitelyRunningAsRoot()) {
            throw new RuntimeException("can not run elasticsearch as root");
        }

        if (systemCallFilter) {
            /*
             * Try to install system call filters; if they fail to install; a bootstrap check will fail startup in production mode.
             *
             * TODO: should we fail hard here if system call filters fail to install, or remain lenient in non-production environments?
             */
            Natives.tryInstallSystemCallFilter(tmpFile);
        }

        // mlockall if requested
        if (mlockAll) {
            if (Constants.WINDOWS) {
                Natives.tryVirtualLock();
            } else {
                Natives.tryMlockall();
            }
        }

        // listener for windows close event
        if (ctrlHandler) {
            Natives.addConsoleCtrlHandler(new ConsoleCtrlHandler() {
                @Override
                public boolean handle(int code) {
                    if (CTRL_CLOSE_EVENT == code) {
                        logger.info("running graceful exit on windows");
                        try {
                            Bootstrap.stop();
                        } catch (IOException e) {
                            throw new ElasticsearchException("failed to stop node", e);
                        }
                        return true;
                    }
                    return false;
                }
            });
        }

        // force remainder of JNA to be loaded (if available).
        try {
            JNAKernel32Library.getInstance();
        } catch (Exception ignored) {
            // we've already logged this.
        }

        Natives.trySetMaxNumberOfThreads();
        Natives.trySetMaxSizeVirtualMemory();
        Natives.trySetMaxFileSize();

        // init lucene random seed. it will use /dev/urandom where available:
        StringHelper.randomId();

        // init filesystem natives
        FileSystemNatives.init();
    }

    static void initializeProbes() {
        // Force probes to be loaded
        ProcessProbe.getInstance();
        OsProbe.getInstance();
        JvmInfo.jvmInfo();
        HotThreads.initializeRuntimeMonitoring();
    }

    private void setup(boolean addShutdownHook, Environment environment) throws BootstrapException {
        Settings settings = environment.settings();

        try {
            spawner.spawnNativeControllers(environment, true);
        } catch (IOException e) {
            throw new BootstrapException(e);
        }

        try {
            environment.validateNativesConfig(); // temporary directories are important for JNA
        } catch (IOException e) {
            throw new BootstrapException(e);
        }
        initializeNatives(
            environment.tmpFile(),
            BootstrapSettings.MEMORY_LOCK_SETTING.get(settings),
            true, // always install system call filters, not user-configurable since 8.0.0
            BootstrapSettings.CTRLHANDLER_SETTING.get(settings)
        );

        // initialize probes before the security manager is installed
        initializeProbes();

        if (addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        IOUtils.close(node, spawner);
                        LoggerContext context = (LoggerContext) LogManager.getContext(false);
                        Configurator.shutdown(context);
                        if (node != null && node.awaitClose(10, TimeUnit.SECONDS) == false) {
                            throw new IllegalStateException(
                                "Node didn't stop within 10 seconds. " + "Any outstanding requests or tasks might get killed."
                            );
                        }
                    } catch (IOException ex) {
                        throw new ElasticsearchException("failed to stop node", ex);
                    } catch (InterruptedException e) {
                        LogManager.getLogger(Bootstrap.class).warn("Thread got interrupted while waiting for the node to shutdown.");
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        try {
            // look for jar hell
            final Logger logger = LogManager.getLogger(JarHell.class);
            JarHell.checkJarHell(logger::debug);
        } catch (IOException | URISyntaxException e) {
            throw new BootstrapException(e);
        }

        // Log ifconfig output before SecurityManager is installed
        IfConfig.logIfNecessary();

        // install SM after natives, shutdown hooks, etc.
        try {
            Security.configure(environment, BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING.get(settings));
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new BootstrapException(e);
        }

        node = new Node(environment) {
            @Override
            protected void validateNodeBeforeAcceptingRequests(
                final BootstrapContext context,
                final BoundTransportAddress boundTransportAddress,
                List<BootstrapCheck> checks
            ) throws NodeValidationException {
                BootstrapChecks.check(context, boundTransportAddress, checks);
            }
        };
    }

    // visible for tests

    private static Environment createEnvironment(
        final Path pidFile,
        final SecureSettings secureSettings,
        final Settings initialSettings,
        final Path configPath
    ) {
        Settings.Builder builder = Settings.builder();
        if (pidFile != null) {
            builder.put(Environment.NODE_PIDFILE_SETTING.getKey(), pidFile);
        }
        builder.put(initialSettings);
        if (secureSettings != null) {
            builder.setSecureSettings(secureSettings);
        }
        return InternalSettingsPreparer.prepareEnvironment(
            builder.build(),
            Collections.emptyMap(),
            configPath,
            // HOSTNAME is set by elasticsearch-env and elasticsearch-env.bat so it is always available
            () -> System.getenv("HOSTNAME")
        );
    }

    private void start() throws NodeValidationException {
        node.start();
        keepAliveThread.start();
    }

    static void stop() throws IOException {
        try {
            IOUtils.close(INSTANCE.node, INSTANCE.spawner);
            if (INSTANCE.node != null && INSTANCE.node.awaitClose(10, TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("Node didn't stop within 10 seconds. Any outstanding requests or tasks might get killed.");
            }
        } catch (InterruptedException e) {
            LogManager.getLogger(Bootstrap.class).warn("Thread got interrupted while waiting for the node to shutdown.");
            Thread.currentThread().interrupt();
        } finally {
            INSTANCE.keepAliveLatch.countDown();
        }
    }

    /**
     * This method is invoked by {@link Elasticsearch#main(String[])} to startup elasticsearch.
     */
    static void init(final boolean foreground, final Path pidFile, final boolean quiet, final Environment initialEnv)
        throws BootstrapException, NodeValidationException, UserException {
        // force the class initializer for BootstrapInfo to run before
        // the security manager is installed
        BootstrapInfo.init();

        INSTANCE = new Bootstrap();

        final SecureSettings keystore = BootstrapUtil.loadSecureSettings(initialEnv);
        final Environment environment = createEnvironment(pidFile, keystore, initialEnv.settings(), initialEnv.configFile());

        BootstrapInfo.setConsole(getConsole(environment));

        // the LogConfigurator will replace System.out and System.err with redirects to our logfile, so we need to capture
        // the stream objects before calling LogConfigurator to be able to close them when appropriate
        final Runnable sysOutCloser = getSysOutCloser();
        final Runnable sysErrorCloser = getSysErrorCloser();

        LogConfigurator.setNodeName(Node.NODE_NAME_SETTING.get(environment.settings()));
        try {
            LogConfigurator.configure(environment);
        } catch (IOException e) {
            throw new BootstrapException(e);
        }
        if (environment.pidFile() != null) {
            try {
                PidFile.create(environment.pidFile(), true);
            } catch (IOException e) {
                throw new BootstrapException(e);
            }
        }

        try {
            final boolean closeStandardStreams = (foreground == false) || quiet;
            if (closeStandardStreams) {
                final Logger rootLogger = LogManager.getRootLogger();
                final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
                if (maybeConsoleAppender != null) {
                    Loggers.removeAppender(rootLogger, maybeConsoleAppender);
                }
                sysOutCloser.run();
            }

            // fail if somebody replaced the lucene jars
            checkLucene();

            // install the default uncaught exception handler; must be done before security is
            // initialized as we do not want to grant the runtime permission
            // setDefaultUncaughtExceptionHandler
            Thread.setDefaultUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler());

            if (PluginsManager.configExists(environment)) {
                if (Build.CURRENT.type() == Build.Type.DOCKER) {
                    try {
                        PluginsManager.syncPlugins(environment);
                    } catch (Exception e) {
                        throw new BootstrapException(e);
                    }
                } else {
                    throw new BootstrapException(
                        new ElasticsearchException("Can only use [elasticsearch-plugins.yml] config file with distribution type [docker]")
                    );
                }
            }

            INSTANCE.setup(true, environment);

            try {
                // any secure settings must be read during node construction
                IOUtils.close(keystore);
            } catch (IOException e) {
                throw new BootstrapException(e);
            }

            INSTANCE.start();

            // We don't close stderr if `--quiet` is passed, because that
            // hides fatal startup errors. For example, if Elasticsearch is
            // running via systemd, the init script only specifies
            // `--quiet`, not `-d`, so we want users to be able to see
            // startup errors via journalctl.
            if (foreground == false) {
                sysErrorCloser.run();
            }

        } catch (NodeValidationException | RuntimeException e) {
            // disable console logging, so user does not see the exception twice (jvm will show it already)
            final Logger rootLogger = LogManager.getRootLogger();
            final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
            if (foreground && maybeConsoleAppender != null) {
                Loggers.removeAppender(rootLogger, maybeConsoleAppender);
            }
            Logger logger = LogManager.getLogger(Bootstrap.class);
            // HACK, it sucks to do this, but we will run users out of disk space otherwise
            if (e instanceof CreationException) {
                // guice: log the shortened exc to the log file
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream ps = null;
                try {
                    ps = new PrintStream(os, false, "UTF-8");
                } catch (UnsupportedEncodingException uee) {
                    assert false;
                    e.addSuppressed(uee);
                }
                new StartupException(e).printStackTrace(ps);
                ps.flush();
                try {
                    logger.error("Guice Exception: {}", os.toString("UTF-8"));
                } catch (UnsupportedEncodingException uee) {
                    assert false;
                    e.addSuppressed(uee);
                }
            } else if (e instanceof NodeValidationException) {
                logger.error("node validation exception\n{}", e.getMessage());
            } else {
                // full exception
                logger.error("Exception", e);
            }
            // re-enable it if appropriate, so they can see any logging during the shutdown process
            if (foreground && maybeConsoleAppender != null) {
                Loggers.addAppender(rootLogger, maybeConsoleAppender);
            }

            throw e;
        }
    }

    private static ConsoleLoader.Console getConsole(Environment environment) {
        return ConsoleLoader.loadConsole(environment);
    }

    @SuppressForbidden(reason = "System#out")
    private static Runnable getSysOutCloser() {
        return System.out::close;
    }

    @SuppressForbidden(reason = "System#err")
    private static Runnable getSysErrorCloser() {
        return System.err::close;
    }

    private static void checkLucene() {
        if (Version.CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST) == false) {
            throw new AssertionError(
                "Lucene version mismatch this version of Elasticsearch requires lucene version ["
                    + Version.CURRENT.luceneVersion
                    + "]  but the current lucene version is ["
                    + org.apache.lucene.util.Version.LATEST
                    + "]"
            );
        }
    }

}
