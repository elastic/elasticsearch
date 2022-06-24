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
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.Elasticsearch.BootstrapState;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.filesystem.FileSystemNatives;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Internal startup code.
 */
final class Bootstrap {

    static volatile Bootstrap INSTANCE;
    private volatile Node node;
    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);
    private final Thread keepAliveThread;
    private final Spawner spawner;

    /** creates a new instance */
    Bootstrap(Spawner spawner) {
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
        this.spawner = spawner;
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
                        if (Bootstrap.INSTANCE != null) {
                            Bootstrap.INSTANCE.shutdown();
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

    private void start() throws NodeValidationException {
        node.start();
        keepAliveThread.start();
    }

    void shutdown() {
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
        } finally {
            keepAliveLatch.countDown();
        }
    }

    /**
     * This method is invoked by {@link Elasticsearch#main(String[])} to startup elasticsearch.
     */
    static void init(ServerArgs args, BootstrapState state) throws NodeValidationException, IOException, UserException {

        INSTANCE = new Bootstrap(state.spawner());

        // fail if somebody replaced the lucene jars
        checkLucene();

        INSTANCE.node = new Node(state.environment()) {
            @Override
            protected void validateNodeBeforeAcceptingRequests(
                final BootstrapContext context,
                final BoundTransportAddress boundTransportAddress,
                List<BootstrapCheck> checks
            ) throws NodeValidationException {
                BootstrapChecks.check(context, boundTransportAddress, checks);
            }
        };

        // any secure settings must be read during node construction
        IOUtils.close(state.secureSettings());

        INSTANCE.start();

        if (args.daemonize()) {
            LogConfigurator.removeConsoleAppender();
        }
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
