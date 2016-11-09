/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * We enforce bootstrap checks once a node has the transport protocol bound to a non-loopback interface. In this case we assume the node is
 * running in production and all bootstrap checks must pass.
 */
final class BootstrapCheck {

    private BootstrapCheck() {
    }

    /**
     * Executes the bootstrap checks if the node has the transport protocol bound to a non-loopback interface.
     *
     * @param settings              the current node settings
     * @param boundTransportAddress the node network bindings
     */
    static void check(final Settings settings, final BoundTransportAddress boundTransportAddress) throws NodeValidationException {
        check(
                enforceLimits(boundTransportAddress),
                checks(settings),
                Node.NODE_NAME_SETTING.get(settings));
    }

    /**
     * Executes the provided checks and fails the node if {@code enforceLimits} is {@code true}, otherwise logs warnings.
     *
     * @param enforceLimits {@code true} if the checks should be enforced or otherwise warned
     * @param checks        the checks to execute
     * @param nodeName      the node name to be used as a logging prefix
     */
    static void check(
        final boolean enforceLimits,
        final List<Check> checks,
        final String nodeName) throws NodeValidationException {
        check(enforceLimits, checks, Loggers.getLogger(BootstrapCheck.class, nodeName));
    }

    /**
     * Executes the provided checks and fails the node if {@code enforceLimits} is {@code true}, otherwise logs warnings.
     *
     * @param enforceLimits {@code true} if the checks should be enforced or otherwise warned
     * @param checks        the checks to execute
     * @param logger        the logger to
     */
    static void check(
            final boolean enforceLimits,
            final List<Check> checks,
            final Logger logger) throws NodeValidationException {
        final List<String> errors = new ArrayList<>();
        final List<String> ignoredErrors = new ArrayList<>();

        if (enforceLimits) {
            logger.info("bound or publishing to a non-loopback or non-link-local address, enforcing bootstrap checks");
        }

        for (final Check check : checks) {
            if (check.check()) {
                if (!enforceLimits && !check.alwaysEnforce()) {
                    ignoredErrors.add(check.errorMessage());
                } else {
                    errors.add(check.errorMessage());
                }
            }
        }

        if (!ignoredErrors.isEmpty()) {
            ignoredErrors.forEach(error -> log(logger, error));
        }

        if (!errors.isEmpty()) {
            final List<String> messages = new ArrayList<>(1 + errors.size());
            messages.add("bootstrap checks failed");
            messages.addAll(errors);
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
     * @return {@code true} if the checks should be enforced
     */
    static boolean enforceLimits(BoundTransportAddress boundTransportAddress) {
        return !(Arrays.stream(boundTransportAddress.boundAddresses()).allMatch(TransportAddress::isLoopbackOrLinkLocalAddress) &&
                boundTransportAddress.publishAddress().isLoopbackOrLinkLocalAddress());
    }

    // the list of checks to execute
    static List<Check> checks(final Settings settings) {
        final List<Check> checks = new ArrayList<>();
        checks.add(new HeapSizeCheck());
        final FileDescriptorCheck fileDescriptorCheck
            = Constants.MAC_OS_X ? new OsXFileDescriptorCheck() : new FileDescriptorCheck();
        checks.add(fileDescriptorCheck);
        checks.add(new MlockallCheck(BootstrapSettings.MEMORY_LOCK_SETTING.get(settings)));
        if (Constants.LINUX) {
            checks.add(new MaxNumberOfThreadsCheck());
        }
        if (Constants.LINUX || Constants.MAC_OS_X) {
            checks.add(new MaxSizeVirtualMemoryCheck());
        }
        if (Constants.LINUX) {
            checks.add(new MaxMapCountCheck());
        }
        checks.add(new ClientJvmCheck());
        checks.add(new UseSerialGCCheck());
        checks.add(new OnErrorCheck());
        checks.add(new OnOutOfMemoryErrorCheck());
        checks.add(new G1GCCheck());
        return Collections.unmodifiableList(checks);
    }

    /**
     * Encapsulates a bootstrap check.
     */
    interface Check {

        /**
         * Test if the node fails the check.
         *
         * @return {@code true} if the node failed the check
         */
        boolean check();

        /**
         * The error message for a failed check.
         *
         * @return the error message on check failure
         */
        String errorMessage();

        default boolean alwaysEnforce() {
            return false;
        }

    }

    static class HeapSizeCheck implements BootstrapCheck.Check {

        @Override
        public boolean check() {
            final long initialHeapSize = getInitialHeapSize();
            final long maxHeapSize = getMaxHeapSize();
            return initialHeapSize != 0 && maxHeapSize != 0 && initialHeapSize != maxHeapSize;
        }

        @Override
        public String errorMessage() {
            return String.format(
                    Locale.ROOT,
                    "initial heap size [%d] not equal to maximum heap size [%d]; " +
                            "this can cause resize pauses and prevents mlockall from locking the entire heap",
                    getInitialHeapSize(),
                    getMaxHeapSize()
            );
        }

        // visible for testing
        long getInitialHeapSize() {
            return JvmInfo.jvmInfo().getConfiguredInitialHeapSize();
        }

        // visible for testing
        long getMaxHeapSize() {
            return JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        }

    }

    static class OsXFileDescriptorCheck extends FileDescriptorCheck {

        public OsXFileDescriptorCheck() {
            // see constant OPEN_MAX defined in
            // /usr/include/sys/syslimits.h on OS X and its use in JVM
            // initialization in int os:init_2(void) defined in the JVM
            // code for BSD (contains OS X)
            super(10240);
        }

    }

    static class FileDescriptorCheck implements Check {

        private final int limit;

        FileDescriptorCheck() {
            this(1 << 16);
        }

        protected FileDescriptorCheck(final int limit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("limit must be positive but was [" + limit + "]");
            }
            this.limit = limit;
        }

        public final boolean check() {
            final long maxFileDescriptorCount = getMaxFileDescriptorCount();
            return maxFileDescriptorCount != -1 && maxFileDescriptorCount < limit;
        }

        @Override
        public final String errorMessage() {
            return String.format(
                Locale.ROOT,
                "max file descriptors [%d] for elasticsearch process is too low, increase to at least [%d]",
                getMaxFileDescriptorCount(),
                limit
            );
        }

        // visible for testing
        long getMaxFileDescriptorCount() {
            return ProcessProbe.getInstance().getMaxFileDescriptorCount();
        }

    }

    static class MlockallCheck implements Check {

        private final boolean mlockallSet;

        public MlockallCheck(final boolean mlockAllSet) {
            this.mlockallSet = mlockAllSet;
        }

        @Override
        public boolean check() {
            return mlockallSet && !isMemoryLocked();
        }

        @Override
        public String errorMessage() {
            return "memory locking requested for elasticsearch process but memory is not locked";
        }

        // visible for testing
        boolean isMemoryLocked() {
            return Natives.isMemoryLocked();
        }

    }

    static class MaxNumberOfThreadsCheck implements Check {

        private final long maxNumberOfThreadsThreshold = 1 << 11;

        @Override
        public boolean check() {
            return getMaxNumberOfThreads() != -1 && getMaxNumberOfThreads() < maxNumberOfThreadsThreshold;
        }

        @Override
        public String errorMessage() {
            return String.format(
                Locale.ROOT,
                "max number of threads [%d] for user [%s] is too low, increase to at least [%d]",
                getMaxNumberOfThreads(),
                BootstrapInfo.getSystemProperties().get("user.name"),
                maxNumberOfThreadsThreshold);
        }

        // visible for testing
        long getMaxNumberOfThreads() {
            return JNANatives.MAX_NUMBER_OF_THREADS;
        }

    }

    static class MaxSizeVirtualMemoryCheck implements Check {

        @Override
        public boolean check() {
            return getMaxSizeVirtualMemory() != Long.MIN_VALUE && getMaxSizeVirtualMemory() != getRlimInfinity();
        }

        @Override
        public String errorMessage() {
            return String.format(
                Locale.ROOT,
                "max size virtual memory [%d] for user [%s] is too low, increase to [unlimited]",
                getMaxSizeVirtualMemory(),
                BootstrapInfo.getSystemProperties().get("user.name"));
        }

        // visible for testing
        long getRlimInfinity() {
            return JNACLibrary.RLIM_INFINITY;
        }

        // visible for testing
        long getMaxSizeVirtualMemory() {
            return JNANatives.MAX_SIZE_VIRTUAL_MEMORY;
        }

    }

    static class MaxMapCountCheck implements Check {

        private final long limit = 1 << 18;

        @Override
        public boolean check() {
            return getMaxMapCount() != -1 && getMaxMapCount() < limit;
        }

        @Override
        public String errorMessage() {
            return String.format(
                    Locale.ROOT,
                    "max virtual memory areas vm.max_map_count [%d] is too low, increase to at least [%d]",
                    getMaxMapCount(),
                    limit);
        }

        // visible for testing
        long getMaxMapCount() {
            return getMaxMapCount(Loggers.getLogger(BootstrapCheck.class));
        }

        // visible for testing
        long getMaxMapCount(Logger logger) {
            final Path path = getProcSysVmMaxMapCountPath();
            try (final BufferedReader bufferedReader = getBufferedReader(path)) {
                final String rawProcSysVmMaxMapCount = readProcSysVmMaxMapCount(bufferedReader);
                if (rawProcSysVmMaxMapCount != null) {
                    try {
                        return parseProcSysVmMaxMapCount(rawProcSysVmMaxMapCount);
                    } catch (final NumberFormatException e) {
                        logger.warn(
                            (Supplier<?>) () -> new ParameterizedMessage(
                                "unable to parse vm.max_map_count [{}]",
                                rawProcSysVmMaxMapCount),
                            e);
                    }
                }
            } catch (final IOException e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("I/O exception while trying to read [{}]", path), e);
            }
            return -1;
        }

        @SuppressForbidden(reason = "access /proc/sys/vm/max_map_count")
        private Path getProcSysVmMaxMapCountPath() {
            return PathUtils.get("/proc/sys/vm/max_map_count");
        }

        // visible for testing
        BufferedReader getBufferedReader(final Path path) throws IOException {
            return Files.newBufferedReader(path);
        }

        // visible for testing
        String readProcSysVmMaxMapCount(final BufferedReader bufferedReader) throws IOException {
            return bufferedReader.readLine();
        }

        // visible for testing
        long parseProcSysVmMaxMapCount(final String procSysVmMaxMapCount) throws NumberFormatException {
            return Long.parseLong(procSysVmMaxMapCount);
        }

    }

    static class ClientJvmCheck implements BootstrapCheck.Check {

        @Override
        public boolean check() {
            return getVmName().toLowerCase(Locale.ROOT).contains("client");
        }

        // visible for testing
        String getVmName() {
            return JvmInfo.jvmInfo().getVmName();
        }

        @Override
        public String errorMessage() {
            return String.format(
                    Locale.ROOT,
                    "JVM is using the client VM [%s] but should be using a server VM for the best performance",
                    getVmName());
        }

    }

    /**
     * Checks if the serial collector is in use. This collector is single-threaded and devastating
     * for performance and should not be used for a server application like Elasticsearch.
     */
    static class UseSerialGCCheck implements BootstrapCheck.Check {

        @Override
        public boolean check() {
            return getUseSerialGC().equals("true");
        }

        // visible for testing
        String getUseSerialGC() {
            return JvmInfo.jvmInfo().useSerialGC();
        }

        @Override
        public String errorMessage() {
            return String.format(
                Locale.ROOT,
                "JVM is using the serial collector but should not be for the best performance; " +
                    "either it's the default for the VM [%s] or -XX:+UseSerialGC was explicitly specified",
                JvmInfo.jvmInfo().getVmName());
        }

    }

    abstract static class MightForkCheck implements BootstrapCheck.Check {

        @Override
        public boolean check() {
            return isSeccompInstalled() && mightFork();
        }

        // visible for testing
        boolean isSeccompInstalled() {
            return Natives.isSeccompInstalled();
        }

        // visible for testing
        abstract boolean mightFork();

        @Override
        public final boolean alwaysEnforce() {
            return true;
        }

    }

    static class OnErrorCheck extends MightForkCheck {

        @Override
        boolean mightFork() {
            final String onError = onError();
            return onError != null && !onError.equals("");
        }

        // visible for testing
        String onError() {
            return JvmInfo.jvmInfo().onError();
        }

        @Override
        public String errorMessage() {
            return String.format(
                Locale.ROOT,
                "OnError [%s] requires forking but is prevented by system call filters ([%s=true]);" +
                    " upgrade to at least Java 8u92 and use ExitOnOutOfMemoryError",
                onError(),
                BootstrapSettings.SECCOMP_SETTING.getKey());
        }

    }

    static class OnOutOfMemoryErrorCheck extends MightForkCheck {

        @Override
        boolean mightFork() {
            final String onOutOfMemoryError = onOutOfMemoryError();
            return onOutOfMemoryError != null && !onOutOfMemoryError.equals("");
        }

        // visible for testing
        String onOutOfMemoryError() {
            return JvmInfo.jvmInfo().onOutOfMemoryError();
        }

        @Override
        public String errorMessage() {
            return String.format(
                Locale.ROOT,
                "OnOutOfMemoryError [%s] requires forking but is prevented by system call filters ([%s=true]);" +
                    " upgrade to at least Java 8u92 and use ExitOnOutOfMemoryError",
                onOutOfMemoryError(),
                BootstrapSettings.SECCOMP_SETTING.getKey());
        }

    }

    /**
     * Bootstrap check for versions of HotSpot that are known to have issues that can lead to index corruption when G1GC is enabled.
     */
    static class G1GCCheck implements BootstrapCheck.Check {

        @Override
        public boolean check() {
            if ("Oracle Corporation".equals(jvmVendor()) && isJava8() && isG1GCEnabled()) {
                final String jvmVersion = jvmVersion();
                // HotSpot versions on Java 8 match this regular expression; note that this changes with Java 9 after JEP-223
                final Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)-b\\d+");
                final Matcher matcher = pattern.matcher(jvmVersion);
                final boolean matches = matcher.matches();
                assert matches : jvmVersion;
                final int major = Integer.parseInt(matcher.group(1));
                final int update = Integer.parseInt(matcher.group(2));
                // HotSpot versions for Java 8 have major version 25, the bad versions are all versions prior to update 40
                return major == 25 && update < 40;
            } else {
                return false;
            }
        }

        // visible for testing
        String jvmVendor() {
            return Constants.JVM_VENDOR;
        }

        // visible for testing
        boolean isG1GCEnabled() {
            assert "Oracle Corporation".equals(jvmVendor());
            return JvmInfo.jvmInfo().useG1GC().equals("true");
        }

        // visible for testing
        String jvmVersion() {
            assert "Oracle Corporation".equals(jvmVendor());
            return Constants.JVM_VERSION;
        }

        // visible for testing
        boolean isJava8() {
            assert "Oracle Corporation".equals(jvmVendor());
            return JavaVersion.current().equals(JavaVersion.parse("1.8"));
        }

        @Override
        public String errorMessage() {
           return String.format(
               Locale.ROOT,
               "JVM version [%s] can cause data corruption when used with G1GC; upgrade to at least Java 8u40", jvmVersion());
        }

    }

}
