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

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.transport.TransportSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * We enforce limits once any network host is configured. In this case we assume the node is running in production
 * and all production limit checks must pass. This should be extended as we go to settings like:
 * - discovery.zen.minimum_master_nodes
 * - discovery.zen.ping.unicast.hosts is set if we use zen disco
 * - ensure we can write in all data directories
 * - fail if vm.max_map_count is under a certain limit (not sure if this works cross platform)
 * - fail if the default cluster.name is used, if this is setup on network a real clustername should be used?
 */
final class BootstrapCheck {

    private BootstrapCheck() {
    }

    /**
     * checks the current limits against the snapshot or release build
     * checks
     *
     * @param settings the current node settings
     */
    public static void check(final Settings settings) {
        check(enforceLimits(settings), checks(settings));
    }

    /**
     * executes the provided checks and fails the node if
     * enforceLimits is true, otherwise logs warnings
     *
     * @param enforceLimits true if the checks should be enforced or
     *                      warned
     * @param checks        the checks to execute
     */
    // visible for testing
    static void check(final boolean enforceLimits, final List<Check> checks) {
        final ESLogger logger = Loggers.getLogger(BootstrapCheck.class);

        for (final Check check : checks) {
            final boolean fail = check.check();
            if (fail) {
                if (enforceLimits) {
                    throw new RuntimeException(check.errorMessage());
                } else {
                    logger.warn(check.errorMessage());
                }
            }
        }
    }

    /**
     * The set of settings such that if any are set for the node, then
     * the checks are enforced
     *
     * @return the enforcement settings
     */
    // visible for testing
    static Set<Setting> enforceSettings() {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            TransportSettings.BIND_HOST,
            TransportSettings.HOST,
            TransportSettings.PUBLISH_HOST,
            NetworkService.GLOBAL_NETWORK_HOST_SETTING,
            NetworkService.GLOBAL_NETWORK_BINDHOST_SETTING,
            NetworkService.GLOBAL_NETWORK_PUBLISHHOST_SETTING
        )));
    }

    /**
     * Tests if the checks should be enforced
     *
     * @param settings the current node settings
     * @return true if the checks should be enforced
     */
    // visible for testing
    static boolean enforceLimits(final Settings settings) {
        return enforceSettings().stream().anyMatch(s -> s.exists(settings));
    }

    // the list of checks to execute
    private static List<Check> checks(final Settings settings) {
        final List<Check> checks = new ArrayList<>();
        final FileDescriptorCheck fileDescriptorCheck
                = Constants.MAC_OS_X ? new OsXFileDescriptorCheck() : new FileDescriptorCheck();
        checks.add(fileDescriptorCheck);
        checks.add(new MlockallCheck(BootstrapSettings.MLOCKALL_SETTING.get(settings)));
        return Collections.unmodifiableList(checks);
    }

    /**
     * Encapsulates a limit check
     */
    interface Check {

        /**
         * test if the node fails the check
         *
         * @return true if the node failed the check
         */
        boolean check();

        /**
         * the message for a failed check
         *
         * @return the error message on check failure
         */
        String errorMessage();

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

    // visible for testing
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
                    "max file descriptors [%d] for elasticsearch process likely too low, increase to at least [%d]",
                    getMaxFileDescriptorCount(),
                    limit
            );
        }

        // visible for testing
        long getMaxFileDescriptorCount() {
            return ProcessProbe.getInstance().getMaxFileDescriptorCount();
        }

    }

    // visible for testing
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
            return "Memory locking requested for elasticsearch process but memory is not locked";
        }

        // visible for testing
        boolean isMemoryLocked() {
            return Natives.isMemoryLocked();
        }

    }

}
