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
import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.ProcessLimits;
import org.elasticsearch.node.NodeValidationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AllPermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;

/**
 * We enforce bootstrap checks once a node has the transport protocol bound to a non-loopback interface or if the system property {@code
 * es.enforce.bootstrap.checks} is set to {@true}. In this case we assume the node is running in production and all bootstrap checks must
 * pass.
 */
final class BootstrapChecks {

    private BootstrapChecks() {}

    static final String ES_ENFORCE_BOOTSTRAP_CHECKS = "es.enforce.bootstrap.checks";

    /**
     * Executes the bootstrap checks if the node has the transport protocol bound to a non-loopback interface. If the system property
     * {@code es.enforce.bootstrap.checks} is set to {@code true} then the bootstrap checks will be enforced regardless of whether or not
     * the transport protocol is bound to a non-loopback interface.
     *
     * @param context              the current node bootstrap context
     * @param boundTransportAddress the node network bindings
     */
    static void check(
        final BootstrapContext context,
        final BoundTransportAddress boundTransportAddress,
        List<BootstrapCheck> additionalChecks
    ) throws NodeValidationException {
        final List<BootstrapCheck> builtInChecks = checks();
        final List<BootstrapCheck> combinedChecks = new ArrayList<>(builtInChecks);
        combinedChecks.addAll(additionalChecks);
        check(
            context,
            enforceLimits(boundTransportAddress, DiscoveryModule.DISCOVERY_TYPE_SETTING.get(context.settings())),
            Collections.unmodifiableList(combinedChecks)
        );
    }

    /**
     * Executes the provided checks and fails the node if {@code enforceLimits} is {@code true}, otherwise logs warnings. If the system
     * property {@code es.enforce.bootstrap.checks} is set to {@code true} then the bootstrap checks will be enforced regardless of whether
     * or not the transport protocol is bound to a non-loopback interface.
     *
     * @param context        the current node boostrap context
     * @param enforceLimits {@code true} if the checks should be enforced or otherwise warned
     * @param checks        the checks to execute
     */
    static void check(final BootstrapContext context, final boolean enforceLimits, final List<BootstrapCheck> checks)
        throws NodeValidationException {
        check(context, enforceLimits, checks, LogManager.getLogger(BootstrapChecks.class));
    }

    /**
     * Executes the provided checks and fails the node if {@code enforceLimits} is {@code true}, otherwise logs warnings. If the system
     * property {@code es.enforce.bootstrap.checks }is set to {@code true} then the bootstrap checks will be enforced regardless of whether
     * or not the transport protocol is bound to a non-loopback interface.
     *
     * @param context the current node boostrap context
     * @param enforceLimits {@code true} if the checks should be enforced or otherwise warned
     * @param checks        the checks to execute
     * @param logger        the logger to
     */
    static void check(final BootstrapContext context, final boolean enforceLimits, final List<BootstrapCheck> checks, final Logger logger)
        throws NodeValidationException {
        final List<String> errors = new ArrayList<>();
        final List<String> ignoredErrors = new ArrayList<>();

        final String esEnforceBootstrapChecks = System.getProperty(ES_ENFORCE_BOOTSTRAP_CHECKS);
        final boolean enforceBootstrapChecks;
        if (esEnforceBootstrapChecks == null) {
            enforceBootstrapChecks = false;
        } else if (Boolean.TRUE.toString().equals(esEnforceBootstrapChecks)) {
            enforceBootstrapChecks = true;
        } else {
            final String message = String.format(
                Locale.ROOT,
                "[%s] must be [true] but was [%s]",
                ES_ENFORCE_BOOTSTRAP_CHECKS,
                esEnforceBootstrapChecks
            );
            throw new IllegalArgumentException(message);
        }

        if (enforceLimits) {
            logger.info("bound or publishing to a non-loopback address, enforcing bootstrap checks");
        } else if (enforceBootstrapChecks) {
            logger.info("explicitly enforcing bootstrap checks");
        }

        for (final BootstrapCheck check : checks) {
            final BootstrapCheck.BootstrapCheckResult result = check.check(context);
            if (result.isFailure()) {
                final String message = result.getMessage() + "; for more information see [" + check.referenceDocs() + "]";
                if (enforceLimits == false && enforceBootstrapChecks == false && check.alwaysEnforce() == false) {
                    ignoredErrors.add(message);
                } else {
                    errors.add(message);
                }
            }
        }

        if (ignoredErrors.isEmpty() == false) {
            ignoredErrors.forEach(error -> log(logger, error));
        }

        if (errors.isEmpty() == false) {
            final List<String> messages = new ArrayList<>(1 + errors.size());
            messages.add(
                "["
                    + errors.size()
                    + "] bootstrap checks failed. You must address the points described in the following ["
                    + errors.size()
                    + "] lines before starting Elasticsearch. For more information see ["
                    + ReferenceDocs.BOOTSTRAP_CHECKS
                    + "]"
            );
            for (int i = 0; i < errors.size(); i++) {
                messages.add("bootstrap check failure [" + (i + 1) + "] of [" + errors.size() + "]: " + errors.get(i));
            }
            final NodeValidationException ne = new NodeValidationException(String.join("\n", messages));
            errors.stream().map(IllegalStateException::new).forEach(ne::addSuppressed);
            throw ne;
        }
    }

    static void log(final Logger logger, final String error) {
        logger.warn(error);
    }

    /**
     * Tests if the checks should be enforced.
     *
     * @param boundTransportAddress the node network bindings
     * @param discoveryType the discovery type
     * @return {@code true} if the checks should be enforced
     */
    static boolean enforceLimits(final BoundTransportAddress boundTransportAddress, final String discoveryType) {
        final Predicate<TransportAddress> isLoopbackAddress = t -> t.address().getAddress().isLoopbackAddress();
        final boolean bound = (Arrays.stream(boundTransportAddress.boundAddresses()).allMatch(isLoopbackAddress)
            && isLoopbackAddress.test(boundTransportAddress.publishAddress())) == false;
        return bound && SINGLE_NODE_DISCOVERY_TYPE.equals(discoveryType) == false;
    }

    // the list of checks to execute
    static List<BootstrapCheck> checks() {
        final List<BootstrapCheck> checks = new ArrayList<>();
        checks.add(new HeapSizeCheck());
        final FileDescriptorCheck fileDescriptorCheck = Constants.MAC_OS_X ? new OsXFileDescriptorCheck() : new FileDescriptorCheck();
        checks.add(fileDescriptorCheck);
        checks.add(new MlockallCheck());
        if (Constants.LINUX) {
            checks.add(new MaxNumberOfThreadsCheck());
        }
        if (Constants.LINUX || Constants.MAC_OS_X) {
            checks.add(new MaxSizeVirtualMemoryCheck());
        }
        if (Constants.LINUX || Constants.MAC_OS_X) {
            checks.add(new MaxFileSizeCheck());
        }
        if (Constants.LINUX) {
            checks.add(new MaxMapCountCheck());
        }
        checks.add(new ClientJvmCheck());
        checks.add(new UseSerialGCCheck());
        checks.add(new SystemCallFilterCheck());
        checks.add(new OnErrorCheck());
        checks.add(new OnOutOfMemoryErrorCheck());
        checks.add(new EarlyAccessCheck());
        checks.add(new AllPermissionCheck());
        checks.add(new DiscoveryConfiguredCheck());
        checks.add(new ByteOrderCheck());
        return Collections.unmodifiableList(checks);
    }

    static class HeapSizeCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            final long initialHeapSize = getInitialHeapSize();
            final long maxHeapSize = getMaxHeapSize();
            if (initialHeapSize != 0 && maxHeapSize != 0 && initialHeapSize != maxHeapSize) {
                final String message;
                if (isMemoryLocked()) {
                    message = String.format(
                        Locale.ROOT,
                        "initial heap size [%d] not equal to maximum heap size [%d]; "
                            + "this can cause resize pauses and prevents memory locking from locking the entire heap",
                        getInitialHeapSize(),
                        getMaxHeapSize()
                    );
                } else {
                    message = String.format(
                        Locale.ROOT,
                        "initial heap size [%d] not equal to maximum heap size [%d]; " + "this can cause resize pauses",
                        getInitialHeapSize(),
                        getMaxHeapSize()
                    );
                }
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_HEAP_SIZE;
        }

        // visible for testing
        long getInitialHeapSize() {
            return JvmInfo.jvmInfo().getConfiguredInitialHeapSize();
        }

        // visible for testing
        long getMaxHeapSize() {
            return JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        }

        boolean isMemoryLocked() {
            return NativeAccess.instance().isMemoryLocked();
        }

    }

    static class OsXFileDescriptorCheck extends FileDescriptorCheck {

        OsXFileDescriptorCheck() {
            // see constant OPEN_MAX defined in
            // /usr/include/sys/syslimits.h on OS X and its use in JVM
            // initialization in int os:init_2(void) defined in the JVM
            // code for BSD (contains OS X)
            super(10240);
        }

    }

    static class FileDescriptorCheck implements BootstrapCheck {

        private final int limit;

        FileDescriptorCheck() {
            this(65535);
        }

        protected FileDescriptorCheck(final int limit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("limit must be positive but was [" + limit + "]");
            }
            this.limit = limit;
        }

        public final BootstrapCheckResult check(BootstrapContext context) {
            final long maxFileDescriptorCount = getMaxFileDescriptorCount();
            if (maxFileDescriptorCount != -1 && maxFileDescriptorCount < limit) {
                final String message = String.format(
                    Locale.ROOT,
                    "max file descriptors [%d] for elasticsearch process is too low, increase to at least [%d]",
                    getMaxFileDescriptorCount(),
                    limit
                );
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_FILE_DESCRIPTOR;
        }

        // visible for testing
        long getMaxFileDescriptorCount() {
            return ProcessProbe.getMaxFileDescriptorCount();
        }

    }

    static class MlockallCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            if (BootstrapSettings.MEMORY_LOCK_SETTING.get(context.settings()) && isMemoryLocked() == false) {
                return BootstrapCheckResult.failure("memory locking requested for elasticsearch process but memory is not locked");
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        boolean isMemoryLocked() {
            return NativeAccess.instance().isMemoryLocked();
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_MEMORY_LOCK;
        }

    }

    static class MaxNumberOfThreadsCheck implements BootstrapCheck {

        // this should be plenty for machines up to 256 cores
        private static final long MAX_NUMBER_OF_THREADS_THRESHOLD = 1 << 12;

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            if (getMaxNumberOfThreads() != ProcessLimits.UNKNOWN && getMaxNumberOfThreads() < MAX_NUMBER_OF_THREADS_THRESHOLD) {
                final String message = String.format(
                    Locale.ROOT,
                    "max number of threads [%d] for user [%s] is too low, increase to at least [%d]",
                    getMaxNumberOfThreads(),
                    BootstrapInfo.getSystemProperties().get("user.name"),
                    MAX_NUMBER_OF_THREADS_THRESHOLD
                );
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        long getMaxNumberOfThreads() {
            return NativeAccess.instance().getProcessLimits().maxThreads();
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_MAX_NUMBER_THREADS;
        }
    }

    static class MaxSizeVirtualMemoryCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            if (getMaxSizeVirtualMemory() != Long.MIN_VALUE && getMaxSizeVirtualMemory() != ProcessLimits.UNLIMITED) {
                final String message = String.format(
                    Locale.ROOT,
                    "max size virtual memory [%d] for user [%s] is too low, increase to [unlimited]",
                    getMaxSizeVirtualMemory(),
                    BootstrapInfo.getSystemProperties().get("user.name")
                );
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        long getMaxSizeVirtualMemory() {
            return NativeAccess.instance().getProcessLimits().maxVirtualMemorySize();
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_MAX_SIZE_VIRTUAL_MEMORY;
        }
    }

    /**
     * Bootstrap check that the maximum file size is unlimited (otherwise Elasticsearch could run in to an I/O exception writing files).
     */
    static class MaxFileSizeCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            final long maxFileSize = getMaxFileSize();
            if (maxFileSize != Long.MIN_VALUE && maxFileSize != ProcessLimits.UNLIMITED) {
                final String message = String.format(
                    Locale.ROOT,
                    "max file size [%d] for user [%s] is too low, increase to [unlimited]",
                    getMaxFileSize(),
                    BootstrapInfo.getSystemProperties().get("user.name")
                );
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        long getMaxFileSize() {
            return NativeAccess.instance().getProcessLimits().maxVirtualMemorySize();
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_MAX_FILE_SIZE;
        }
    }

    static class MaxMapCountCheck implements BootstrapCheck {

        static final long LIMIT = 1 << 18;

        @Override
        public BootstrapCheckResult check(final BootstrapContext context) {
            // we only enforce the check if a store is allowed to use mmap at all
            if (IndexModule.NODE_STORE_ALLOW_MMAP.get(context.settings())) {
                if (getMaxMapCount() != -1 && getMaxMapCount() < LIMIT) {
                    final String message = String.format(
                        Locale.ROOT,
                        "max virtual memory areas vm.max_map_count [%d] is too low, increase to at least [%d]",
                        getMaxMapCount(),
                        LIMIT
                    );
                    return BootstrapCheckResult.failure(message);
                } else {
                    return BootstrapCheckResult.success();
                }
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        long getMaxMapCount() {
            return getMaxMapCount(LogManager.getLogger(BootstrapChecks.class));
        }

        // visible for testing
        long getMaxMapCount(Logger logger) {
            final Path path = getProcSysVmMaxMapCountPath();
            try (BufferedReader bufferedReader = getBufferedReader(path)) {
                final String rawProcSysVmMaxMapCount = readProcSysVmMaxMapCount(bufferedReader);
                if (rawProcSysVmMaxMapCount != null) {
                    try {
                        return parseProcSysVmMaxMapCount(rawProcSysVmMaxMapCount);
                    } catch (final NumberFormatException e) {
                        logger.warn(() -> "unable to parse vm.max_map_count [" + rawProcSysVmMaxMapCount + "]", e);
                    }
                }
            } catch (final IOException e) {
                logger.warn(() -> "I/O exception while trying to read [" + path + "]", e);
            }
            return -1;
        }

        @SuppressForbidden(reason = "access /proc/sys/vm/max_map_count")
        private static Path getProcSysVmMaxMapCountPath() {
            return PathUtils.get("/proc/sys/vm/max_map_count");
        }

        // visible for testing
        BufferedReader getBufferedReader(final Path path) throws IOException {
            return Files.newBufferedReader(path);
        }

        // visible for testing
        static String readProcSysVmMaxMapCount(final BufferedReader bufferedReader) throws IOException {
            return bufferedReader.readLine();
        }

        // visible for testing
        static long parseProcSysVmMaxMapCount(final String procSysVmMaxMapCount) throws NumberFormatException {
            return Long.parseLong(procSysVmMaxMapCount);
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_MAXIMUM_MAP_COUNT;
        }
    }

    static class ClientJvmCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            if (getVmName().toLowerCase(Locale.ROOT).contains("client")) {
                final String message = String.format(
                    Locale.ROOT,
                    "JVM is using the client VM [%s] but should be using a server VM for the best performance",
                    getVmName()
                );
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        String getVmName() {
            return JvmInfo.jvmInfo().getVmName();
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_CLIENT_JVM;
        }
    }

    /**
     * Checks if the serial collector is in use. This collector is single-threaded and devastating
     * for performance and should not be used for a server application like Elasticsearch.
     */
    static class UseSerialGCCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            if (getUseSerialGC().equals("true")) {
                final String message = String.format(
                    Locale.ROOT,
                    "JVM is using the serial collector but should not be for the best performance; "
                        + "either it's the default for the VM [%s] or -XX:+UseSerialGC was explicitly specified",
                    JvmInfo.jvmInfo().getVmName()
                );
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        String getUseSerialGC() {
            return JvmInfo.jvmInfo().useSerialGC();
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_USE_SERIAL_COLLECTOR;
        }
    }

    /**
     * Bootstrap check that system call filters must have installed successfully.
     */
    static class SystemCallFilterCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            if (isSystemCallFilterInstalled() == false) {
                final String message = "system call filters failed to install; check the logs and fix your configuration";
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        boolean isSystemCallFilterInstalled() {
            return NativeAccess.instance().getExecSandboxState() != NativeAccess.ExecSandboxState.NONE;
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_SYSTEM_CALL_FILTER;
        }
    }

    abstract static class MightForkCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            if (isSystemCallFilterInstalled() && mightFork()) {
                return BootstrapCheckResult.failure(message(context));
            } else {
                return BootstrapCheckResult.success();
            }
        }

        abstract String message(BootstrapContext context);

        // visible for testing
        boolean isSystemCallFilterInstalled() {
            return NativeAccess.instance().getExecSandboxState() != NativeAccess.ExecSandboxState.NONE;
        }

        // visible for testing
        abstract boolean mightFork();

        @Override
        public final boolean alwaysEnforce() {
            return true;
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_ONERROR_AND_ONOUTOFMEMORYERROR;
        }

    }

    static class OnErrorCheck extends MightForkCheck {

        @Override
        boolean mightFork() {
            final String onError = onError();
            return onError != null && onError.isEmpty() == false;
        }

        // visible for testing
        String onError() {
            return JvmInfo.jvmInfo().onError();
        }

        @Override
        String message(BootstrapContext context) {
            return String.format(
                Locale.ROOT,
                "OnError [%s] requires forking but is prevented by system call filters;"
                    + " upgrade to at least Java 8u92 and use ExitOnOutOfMemoryError",
                onError()
            );
        }

    }

    static class OnOutOfMemoryErrorCheck extends MightForkCheck {

        @Override
        boolean mightFork() {
            final String onOutOfMemoryError = onOutOfMemoryError();
            return onOutOfMemoryError != null && onOutOfMemoryError.isEmpty() == false;
        }

        // visible for testing
        String onOutOfMemoryError() {
            return JvmInfo.jvmInfo().onOutOfMemoryError();
        }

        String message(BootstrapContext context) {
            return String.format(
                Locale.ROOT,
                "OnOutOfMemoryError [%s] requires forking but is prevented by system call filters;"
                    + " upgrade to at least Java 8u92 and use ExitOnOutOfMemoryError",
                onOutOfMemoryError()
            );
        }

    }

    /**
     * Bootstrap check for early-access builds from OpenJDK.
     */
    static class EarlyAccessCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            final String javaVersion = javaVersion();
            if ("Oracle Corporation".equals(jvmVendor()) && javaVersion.endsWith("-ea")) {
                final String message = String.format(
                    Locale.ROOT,
                    "Java version [%s] is an early-access build, only use release builds",
                    javaVersion
                );
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        String jvmVendor() {
            return Constants.JVM_VENDOR;
        }

        String javaVersion() {
            return Constants.JAVA_VERSION;
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_EARLY_ACCESS;
        }

    }

    static class AllPermissionCheck implements BootstrapCheck {

        @Override
        public final BootstrapCheckResult check(BootstrapContext context) {
            if (isAllPermissionGranted()) {
                return BootstrapCheck.BootstrapCheckResult.failure("granting the all permission effectively disables security");
            }
            return BootstrapCheckResult.success();
        }

        boolean isAllPermissionGranted() {
            final SecurityManager sm = System.getSecurityManager();
            assert sm != null;
            try {
                sm.checkPermission(new AllPermission());
            } catch (final SecurityException e) {
                return false;
            }
            return true;
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_ALL_PERMISSION;
        }
    }

    static class DiscoveryConfiguredCheck implements BootstrapCheck {
        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            if (DiscoveryModule.MULTI_NODE_DISCOVERY_TYPE.equals(DiscoveryModule.DISCOVERY_TYPE_SETTING.get(context.settings())) == false) {
                return BootstrapCheckResult.success();
            }
            if (ClusterBootstrapService.discoveryIsConfigured(context.settings())) {
                return BootstrapCheckResult.success();
            }

            return BootstrapCheckResult.failure(
                String.format(
                    Locale.ROOT,
                    "the default discovery settings are unsuitable for production use; at least one of [%s] must be configured",
                    Stream.of(DISCOVERY_SEED_HOSTS_SETTING, DISCOVERY_SEED_PROVIDERS_SETTING, INITIAL_MASTER_NODES_SETTING)
                        .map(Setting::getKey)
                        .collect(Collectors.joining(", "))
                )
            );
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECK_DISCOVERY_CONFIGURATION;
        }
    }

    static class ByteOrderCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(BootstrapContext context) {
            if (nativeByteOrder() != ByteOrder.LITTLE_ENDIAN) {
                return BootstrapCheckResult.failure("Little-endian native byte order is required to run Elasticsearch");
            }
            return BootstrapCheckResult.success();
        }

        ByteOrder nativeByteOrder() {
            return ByteOrder.nativeOrder();
        }

        @Override
        public ReferenceDocs referenceDocs() {
            return ReferenceDocs.BOOTSTRAP_CHECKS;
        }
    }
}
