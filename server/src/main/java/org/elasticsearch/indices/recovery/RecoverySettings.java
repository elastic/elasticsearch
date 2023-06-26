/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.store.RateLimiter.SimpleRateLimiter;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AdjustableSemaphore;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.node.NodeRoleSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.Setting.parseInt;

public class RecoverySettings {
    public static final Version SNAPSHOT_RECOVERIES_SUPPORTED_VERSION = Version.V_7_15_0;
    public static final Version SEQ_NO_SNAPSHOT_RECOVERIES_SUPPORTED_VERSION = Version.V_7_16_0;
    public static final Version SNAPSHOT_FILE_DOWNLOAD_THROTTLING_SUPPORTED_VERSION = Version.V_7_16_0;

    private static final Logger logger = LogManager.getLogger(RecoverySettings.class);

    /**
     * Undocumented setting, used to override the total physical available memory in tests
     **/
    // package private for tests
    static final Setting<ByteSizeValue> TOTAL_PHYSICAL_MEMORY_OVERRIDING_TEST_SETTING = Setting.byteSizeSetting(
        "recovery_settings.total_physical_memory_override",
        settings -> new ByteSizeValue(OsProbe.getInstance().getTotalPhysicalMemorySize()).getStringRep(),
        Property.NodeScope
    );

    /**
     * Undocumented setting, used to override the current JVM version in tests
     **/
    // package private for tests
    static final Setting<JavaVersion> JAVA_VERSION_OVERRIDING_TEST_SETTING = new Setting<>(
        "recovery_settings.java_version_override",
        settings -> JavaVersion.current().toString(),
        JavaVersion::parse,
        Property.NodeScope
    );

    /**
     * Disk's write bandwidth allocated for this node. This bandwidth is expressed for write operations that have the default block size of
     * {@link #DEFAULT_CHUNK_SIZE}.
     */
    public static final Setting<ByteSizeValue> NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING = bandwidthSetting(
        "node.bandwidth.recovery.disk.write"
    );

    /**
     * Disk's read bandwidth allocated for this node. This bandwidth is expressed for read operations that have the default block size of
     * {@link #DEFAULT_CHUNK_SIZE}.
     */
    public static final Setting<ByteSizeValue> NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING = bandwidthSetting(
        "node.bandwidth.recovery.disk.read"
    );

    /**
     * Network's read bandwidth allocated for this node.
     */
    public static final Setting<ByteSizeValue> NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING = bandwidthSetting(
        "node.bandwidth.recovery.network"
    );

    static final double DEFAULT_FACTOR_VALUE = 0.4d;

    /**
     * Default factor as defined by the operator.
     */
    public static final Setting<Double> NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_SETTING = operatorFactorSetting(
        "node.bandwidth.recovery.operator.factor",
        DEFAULT_FACTOR_VALUE
    );

    public static final Setting<Double> NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_WRITE_SETTING = operatorFactorSetting(
        "node.bandwidth.recovery.operator.factor.write"
    );

    public static final Setting<Double> NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_READ_SETTING = operatorFactorSetting(
        "node.bandwidth.recovery.operator.factor.read"
    );

    public static final Setting<Double> NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_MAX_OVERCOMMIT_SETTING = Setting.doubleSetting(
        "node.bandwidth.recovery.operator.factor.max_overcommit",
        100d, // high default overcommit
        1d,
        Double.MAX_VALUE,
        Property.NodeScope
    );

    static final List<Setting<?>> NODE_BANDWIDTH_RECOVERY_SETTINGS = Arrays.asList(
        NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING,
        NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING,
        NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING
    );

    /**
     * Bandwidth settings have a default value of -1 (meaning that they are undefined) or a value in (0, Long.MAX_VALUE).
     */
    private static Setting<ByteSizeValue> bandwidthSetting(String key) {
        return new Setting<>(key, s -> ByteSizeValue.MINUS_ONE.getStringRep(), s -> {
            final ByteSizeValue value = ByteSizeValue.parseBytesSizeValue(s, key);
            if (ByteSizeValue.MINUS_ONE.equals(value)) {
                return value;
            }
            if (value.getBytes() <= 0L) {
                throw new IllegalArgumentException(
                    "Failed to parse value ["
                        + s
                        + "] for bandwidth setting ["
                        + key
                        + "], must be > ["
                        + ByteSizeValue.ZERO.getStringRep()
                        + ']'
                );
            }
            if (value.getBytes() >= Long.MAX_VALUE) {
                throw new IllegalArgumentException(
                    "Failed to parse value ["
                        + s
                        + "] for bandwidth setting ["
                        + key
                        + "], must be < ["
                        + new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES).getStringRep()
                        + ']'
                );
            }
            return value;
        }, Property.NodeScope);
    }

    /**
     * Operator-defined factors have a value in (0.0, 1.0]
     */
    private static Setting<Double> operatorFactorSetting(String key, double defaultValue) {
        return new Setting<>(key, Double.toString(defaultValue), s -> Setting.parseDouble(s, 0d, 1d, key), v -> {
            if (v == 0d) {
                throw new IllegalArgumentException("Failed to validate value [" + v + "] for factor setting [" + key + "] must be > [0]");
            }
        }, Property.NodeScope);
    }

    private static Setting<Double> operatorFactorSetting(String key) {
        return new Setting<>(key, NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_SETTING, s -> Setting.parseDouble(s, 0d, 1d, key), v -> {
            if (v == 0d) {
                throw new IllegalArgumentException("Failed to validate value [" + v + "] for factor setting [" + key + "] must be > [0]");
            }
        }, Property.NodeScope);
    }

    static final ByteSizeValue DEFAULT_MAX_BYTES_PER_SEC = new ByteSizeValue(40L, ByteSizeUnit.MB);

    public static final Setting<ByteSizeValue> INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING = Setting.byteSizeSetting(
        "indices.recovery.max_bytes_per_sec",
        s -> {
            final List<DiscoveryNodeRole> roles = NodeRoleSettings.NODE_ROLES_SETTING.get(s);
            final List<DiscoveryNodeRole> dataRoles = roles.stream().filter(DiscoveryNodeRole::canContainData).collect(Collectors.toList());
            if (dataRoles.isEmpty()) {
                // if the node is not a data node, this value doesn't matter, use the default
                return DEFAULT_MAX_BYTES_PER_SEC.getStringRep();
            }
            if (dataRoles.stream()
                .allMatch(
                    dn -> dn.equals(DiscoveryNodeRole.DATA_COLD_NODE_ROLE) || dn.equals(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)
                ) == false) {
                // the node is not a dedicated cold and/or frozen node, use the default
                return DEFAULT_MAX_BYTES_PER_SEC.getStringRep();
            }
            /*
             * Now we are looking at a node that has a single data role, that data role is the cold data role, and the node does not
             * have the master role. In this case, we are going to set the recovery size as a function of the memory size. We are making
             * an assumption here that the size of the instance is correlated with I/O resources. That is we are assuming that the
             * larger the instance, the more disk and networking capacity it has available.
             */
            final JavaVersion javaVersion = JAVA_VERSION_OVERRIDING_TEST_SETTING.get(s);
            if (javaVersion.compareTo(JavaVersion.parse("14")) < 0) {
                // prior to JDK 14, the JDK did not take into consideration container memory limits when reporting total system memory
                return DEFAULT_MAX_BYTES_PER_SEC.getStringRep();
            }
            final ByteSizeValue totalPhysicalMemory = TOTAL_PHYSICAL_MEMORY_OVERRIDING_TEST_SETTING.get(s);
            final ByteSizeValue maxBytesPerSec;
            if (totalPhysicalMemory.compareTo(new ByteSizeValue(4, ByteSizeUnit.GB)) <= 0) {
                maxBytesPerSec = new ByteSizeValue(40, ByteSizeUnit.MB);
            } else if (totalPhysicalMemory.compareTo(new ByteSizeValue(8, ByteSizeUnit.GB)) <= 0) {
                maxBytesPerSec = new ByteSizeValue(60, ByteSizeUnit.MB);
            } else if (totalPhysicalMemory.compareTo(new ByteSizeValue(16, ByteSizeUnit.GB)) <= 0) {
                maxBytesPerSec = new ByteSizeValue(90, ByteSizeUnit.MB);
            } else if (totalPhysicalMemory.compareTo(new ByteSizeValue(32, ByteSizeUnit.GB)) <= 0) {
                maxBytesPerSec = new ByteSizeValue(125, ByteSizeUnit.MB);
            } else {
                maxBytesPerSec = new ByteSizeValue(250, ByteSizeUnit.MB);
            }
            return maxBytesPerSec.getStringRep();
        },
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Controls the maximum number of file chunk requests that can be sent concurrently from the source node to the target node.
     */
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_file_chunks",
        2,
        1,
        8,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Controls the maximum number of operation chunk requests that can be sent concurrently from the source node to the target node.
     */
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_operations",
        1,
        1,
        4,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * how long to wait before retrying after issues cause by cluster state syncing between nodes
     * i.e., local node is not yet known on remote node, remote shard not yet started etc.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING = Setting.positiveTimeSetting(
        "indices.recovery.retry_delay_state_sync",
        TimeValue.timeValueMillis(500),
        Property.Dynamic,
        Property.NodeScope
    );

    /** how long to wait before retrying after network related issues */
    public static final Setting<TimeValue> INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING = Setting.positiveTimeSetting(
        "indices.recovery.retry_delay_network",
        TimeValue.timeValueSeconds(5),
        Property.Dynamic,
        Property.NodeScope
    );

    /** timeout value to use for requests made as part of the recovery process */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "indices.recovery.internal_action_timeout",
        TimeValue.timeValueMinutes(15),
        Property.Dynamic,
        Property.NodeScope
    );

    /** timeout value to use for the retrying of requests made as part of the recovery process */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "indices.recovery.internal_action_retry_timeout",
        TimeValue.timeValueMinutes(1),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * timeout value to use for requests made as part of the recovery process that are expected to take long time.
     * defaults to twice `indices.recovery.internal_action_timeout`.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING = Setting.timeSetting(
        "indices.recovery.internal_action_long_timeout",
        (s) -> TimeValue.timeValueMillis(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.get(s).millis() * 2),
        TimeValue.timeValueSeconds(0),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * recoveries that don't show any activity for more then this interval will be failed.
     * defaults to `indices.recovery.internal_action_long_timeout`
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING = Setting.timeSetting(
        "indices.recovery.recovery_activity_timeout",
        INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING::get,
        TimeValue.timeValueSeconds(0),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * recoveries would try to use files from available snapshots instead of sending them from the source node.
     * defaults to `true`
     */
    public static final Setting<Boolean> INDICES_RECOVERY_USE_SNAPSHOTS_SETTING = Setting.boolSetting(
        "indices.recovery.use_snapshots",
        true,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS = Setting.intSetting(
        "indices.recovery.max_concurrent_snapshot_file_downloads",
        5,
        1,
        20,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE = new Setting<>(
        "indices.recovery.max_concurrent_snapshot_file_downloads_per_node",
        "25",
        (s) -> parseInt(s, 1, 25, "indices.recovery.max_concurrent_snapshot_file_downloads_per_node", false),
        new Setting.Validator<Integer>() {
            private final Collection<Setting<?>> dependencies = Collections.singletonList(
                INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS
            );

            @Override
            public void validate(Integer value) {
                // ignore
            }

            @Override
            public void validate(Integer maxConcurrentSnapshotFileDownloadsPerNode, Map<Setting<?>, Object> settings) {
                int maxConcurrentSnapshotFileDownloads = (int) settings.get(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS);
                if (maxConcurrentSnapshotFileDownloadsPerNode < maxConcurrentSnapshotFileDownloads) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "[%s]=%d is less than [%s]=%d",
                            INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(),
                            maxConcurrentSnapshotFileDownloadsPerNode,
                            INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(),
                            maxConcurrentSnapshotFileDownloads
                        )
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return dependencies.iterator();
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final ByteSizeValue DEFAULT_CHUNK_SIZE = new ByteSizeValue(512, ByteSizeUnit.KB);

    private volatile ByteSizeValue maxBytesPerSec;
    private volatile int maxConcurrentFileChunks;
    private volatile int maxConcurrentOperations;
    private volatile SimpleRateLimiter rateLimiter;
    private volatile TimeValue retryDelayStateSync;
    private volatile TimeValue retryDelayNetwork;
    private volatile TimeValue activityTimeout;
    private volatile TimeValue internalActionTimeout;
    private volatile TimeValue internalActionRetryTimeout;
    private volatile TimeValue internalActionLongTimeout;
    private volatile boolean useSnapshotsDuringRecovery;
    private volatile int maxConcurrentSnapshotFileDownloads;
    private volatile int maxConcurrentSnapshotFileDownloadsPerNode;

    private final AdjustableSemaphore maxSnapshotFileDownloadsPerNodeSemaphore;

    private volatile ByteSizeValue chunkSize = DEFAULT_CHUNK_SIZE;

    private final ByteSizeValue availableNetworkBandwidth;
    private final ByteSizeValue availableDiskReadBandwidth;
    private final ByteSizeValue availableDiskWriteBandwidth;

    public RecoverySettings(Settings settings, ClusterSettings clusterSettings) {
        this.retryDelayStateSync = INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.get(settings);
        this.maxConcurrentFileChunks = INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.get(settings);
        this.maxConcurrentOperations = INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING.get(settings);
        // doesn't have to be fast as nodes are reconnected every 10s by default (see InternalClusterService.ReconnectToNodes)
        // and we want to give the master time to remove a faulty node
        this.retryDelayNetwork = INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.get(settings);

        this.internalActionTimeout = INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.get(settings);
        this.internalActionRetryTimeout = INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING.get(settings);
        this.internalActionLongTimeout = INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING.get(settings);
        this.activityTimeout = INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.get(settings);
        this.useSnapshotsDuringRecovery = INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.get(settings);
        this.maxConcurrentSnapshotFileDownloads = INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.get(settings);
        this.maxConcurrentSnapshotFileDownloadsPerNode = INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.get(settings);
        this.maxSnapshotFileDownloadsPerNodeSemaphore = new AdjustableSemaphore(this.maxConcurrentSnapshotFileDownloadsPerNode, true);
        this.availableNetworkBandwidth = NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING.get(settings);
        this.availableDiskReadBandwidth = NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING.get(settings);
        this.availableDiskWriteBandwidth = NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING.get(settings);
        validateNodeBandwidthRecoverySettings(settings);
        computeMaxBytesPerSec(settings);
        if (DiscoveryNode.canContainData(settings)) {
            clusterSettings.addSettingsUpdateConsumer(
                this::computeMaxBytesPerSec,
                Arrays.asList(
                    INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING,
                    // non dynamic settings but they are used to update max bytes per sec
                    NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_SETTING,
                    NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_READ_SETTING,
                    NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_WRITE_SETTING,
                    NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_MAX_OVERCOMMIT_SETTING,
                    NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING,
                    NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING,
                    NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING
                )
            );
        }
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING, this::setMaxConcurrentFileChunks);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING, this::setMaxConcurrentOperations);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING, this::setRetryDelayStateSync);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING, this::setRetryDelayNetwork);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING, this::setInternalActionTimeout);
        clusterSettings.addSettingsUpdateConsumer(
            INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING,
            this::setInternalActionLongTimeout
        );
        clusterSettings.addSettingsUpdateConsumer(
            INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING,
            this::setInternalActionRetryTimeout
        );
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, this::setActivityTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_USE_SNAPSHOTS_SETTING, this::setUseSnapshotsDuringRecovery);
        clusterSettings.addSettingsUpdateConsumer(
            INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS,
            this::setMaxConcurrentSnapshotFileDownloads
        );
        clusterSettings.addSettingsUpdateConsumer(
            INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE,
            this::setMaxConcurrentSnapshotFileDownloadsPerNode
        );
    }

    private void computeMaxBytesPerSec(Settings settings) {
        // limit as computed before node bandwidth recovery settings were introduced
        final long defaultBytesPerSec = Math.max(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.get(settings).getBytes(), 0L);

        // available network bandwidth
        final long networkBandwidthBytesPerSec = Math.max(availableNetworkBandwidth.getBytes(), 0L);

        // read bandwidth
        final long readBytesPerSec;
        if (availableDiskReadBandwidth.getBytes() > 0L && networkBandwidthBytesPerSec > 0L) {
            double readFactor = NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_READ_SETTING.get(settings);
            readBytesPerSec = Math.round(Math.min(availableDiskReadBandwidth.getBytes(), networkBandwidthBytesPerSec) * readFactor);
        } else {
            readBytesPerSec = 0L;
        }

        // write bandwidth
        final long writeBytesPerSec;
        if (availableDiskWriteBandwidth.getBytes() > 0L && networkBandwidthBytesPerSec > 0L) {
            double writeFactor = NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_WRITE_SETTING.get(settings);
            writeBytesPerSec = Math.round(Math.min(availableDiskWriteBandwidth.getBytes(), networkBandwidthBytesPerSec) * writeFactor);
        } else {
            writeBytesPerSec = 0L;
        }

        final long availableBytesPerSec = Math.min(readBytesPerSec, writeBytesPerSec);

        long maxBytesPerSec;
        if (availableBytesPerSec == 0L                                      // no node recovery bandwidths
            || INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.exists(settings)  // when set this setting overrides node recovery bandwidths
            || DiscoveryNode.canContainData(settings) == false) {           // keep previous behavior for non data nodes
            maxBytesPerSec = defaultBytesPerSec;
        } else {
            maxBytesPerSec = Math.max(defaultBytesPerSec, availableBytesPerSec);
        }

        final long maxAllowedBytesPerSec = Math.round(
            Math.max(
                Math.min(
                    Math.min(availableDiskReadBandwidth.getBytes(), availableDiskWriteBandwidth.getBytes()),
                    networkBandwidthBytesPerSec
                ),
                0L
            ) * NODE_BANDWIDTH_RECOVERY_OPERATOR_FACTOR_MAX_OVERCOMMIT_SETTING.get(settings)
        );

        ByteSizeValue finalMaxBytesPerSec;
        if (maxAllowedBytesPerSec > 0L) {
            if (maxBytesPerSec > 0L) {
                finalMaxBytesPerSec = ByteSizeValue.ofBytes(Math.min(maxBytesPerSec, maxAllowedBytesPerSec));
            } else {
                finalMaxBytesPerSec = ByteSizeValue.ofBytes(maxAllowedBytesPerSec);
            }
        } else {
            finalMaxBytesPerSec = ByteSizeValue.ofBytes(maxBytesPerSec);
        }
        logger.info(
            () -> new ParameterizedMessage(
                "using rate limit [{}] with [default={}, read={}, write={}, max={}]",
                finalMaxBytesPerSec,
                ByteSizeValue.ofBytes(defaultBytesPerSec),
                ByteSizeValue.ofBytes(readBytesPerSec),
                ByteSizeValue.ofBytes(writeBytesPerSec),
                ByteSizeValue.ofBytes(maxAllowedBytesPerSec)
            )
        );
        setMaxBytesPerSec(finalMaxBytesPerSec);
    }

    public RateLimiter rateLimiter() {
        return rateLimiter;
    }

    public TimeValue retryDelayNetwork() {
        return retryDelayNetwork;
    }

    public TimeValue retryDelayStateSync() {
        return retryDelayStateSync;
    }

    public TimeValue activityTimeout() {
        return activityTimeout;
    }

    public TimeValue internalActionTimeout() {
        return internalActionTimeout;
    }

    public TimeValue internalActionRetryTimeout() {
        return internalActionRetryTimeout;
    }

    public TimeValue internalActionLongTimeout() {
        return internalActionLongTimeout;
    }

    public ByteSizeValue getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(ByteSizeValue chunkSize) { // only settable for tests
        if (chunkSize.bytesAsInt() <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
    }

    public void setRetryDelayStateSync(TimeValue retryDelayStateSync) {
        this.retryDelayStateSync = retryDelayStateSync;
    }

    public void setRetryDelayNetwork(TimeValue retryDelayNetwork) {
        this.retryDelayNetwork = retryDelayNetwork;
    }

    public void setActivityTimeout(TimeValue activityTimeout) {
        this.activityTimeout = activityTimeout;
    }

    public void setInternalActionTimeout(TimeValue internalActionTimeout) {
        this.internalActionTimeout = internalActionTimeout;
    }

    public void setInternalActionLongTimeout(TimeValue internalActionLongTimeout) {
        this.internalActionLongTimeout = internalActionLongTimeout;
    }

    public void setInternalActionRetryTimeout(TimeValue internalActionRetryTimeout) {
        this.internalActionRetryTimeout = internalActionRetryTimeout;
    }

    private void setMaxBytesPerSec(ByteSizeValue maxBytesPerSec) {
        this.maxBytesPerSec = maxBytesPerSec;
        if (maxBytesPerSec.getBytes() <= 0) {
            rateLimiter = null;
        } else if (rateLimiter != null) {
            rateLimiter.setMBPerSec(maxBytesPerSec.getMbFrac());
        } else {
            rateLimiter = new SimpleRateLimiter(maxBytesPerSec.getMbFrac());
        }
    }

    ByteSizeValue getMaxBytesPerSec() {
        return maxBytesPerSec;
    }

    public int getMaxConcurrentFileChunks() {
        return maxConcurrentFileChunks;
    }

    private void setMaxConcurrentFileChunks(int maxConcurrentFileChunks) {
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }

    public int getMaxConcurrentOperations() {
        return maxConcurrentOperations;
    }

    private void setMaxConcurrentOperations(int maxConcurrentOperations) {
        this.maxConcurrentOperations = maxConcurrentOperations;
    }

    public boolean getUseSnapshotsDuringRecovery() {
        return useSnapshotsDuringRecovery;
    }

    private void setUseSnapshotsDuringRecovery(boolean useSnapshotsDuringRecovery) {
        this.useSnapshotsDuringRecovery = useSnapshotsDuringRecovery;
    }

    public int getMaxConcurrentSnapshotFileDownloads() {
        return maxConcurrentSnapshotFileDownloads;
    }

    public void setMaxConcurrentSnapshotFileDownloads(int maxConcurrentSnapshotFileDownloads) {
        this.maxConcurrentSnapshotFileDownloads = maxConcurrentSnapshotFileDownloads;
    }

    private void setMaxConcurrentSnapshotFileDownloadsPerNode(int maxConcurrentSnapshotFileDownloadsPerNode) {
        this.maxConcurrentSnapshotFileDownloadsPerNode = maxConcurrentSnapshotFileDownloadsPerNode;
        this.maxSnapshotFileDownloadsPerNodeSemaphore.setMaxPermits(maxConcurrentSnapshotFileDownloadsPerNode);
    }

    @Nullable
    Releasable tryAcquireSnapshotDownloadPermits() {
        if (getUseSnapshotsDuringRecovery() == false) {
            return null;
        }

        final int maxConcurrentSnapshotFileDownloads = getMaxConcurrentSnapshotFileDownloads();
        final boolean permitAcquired = maxSnapshotFileDownloadsPerNodeSemaphore.tryAcquire(maxConcurrentSnapshotFileDownloads);
        if (permitAcquired == false) {
            logger.warn(
                String.format(
                    Locale.ROOT,
                    "Unable to acquire permit to use snapshot files during recovery, "
                        + "this recovery will recover index files from the source node. "
                        + "Ensure snapshot files can be used during recovery by setting [%s] to be no greater than [%d]",
                    INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(),
                    this.maxConcurrentSnapshotFileDownloadsPerNode
                )
            );
            return null;
        }

        return Releasables.releaseOnce(() -> maxSnapshotFileDownloadsPerNodeSemaphore.release(maxConcurrentSnapshotFileDownloads));
    }

    private static void validateNodeBandwidthRecoverySettings(Settings settings) {
        final List<String> nonDefaults = NODE_BANDWIDTH_RECOVERY_SETTINGS.stream()
            .filter(setting -> setting.get(settings) != ByteSizeValue.MINUS_ONE)
            .map(Setting::getKey)
            .collect(Collectors.toList());
        if (nonDefaults.isEmpty() == false && nonDefaults.size() != NODE_BANDWIDTH_RECOVERY_SETTINGS.size()) {
            throw new IllegalArgumentException(
                "Settings "
                    + NODE_BANDWIDTH_RECOVERY_SETTINGS.stream().map(Setting::getKey).collect(Collectors.toList())
                    + " must all be defined or all be undefined; but only settings "
                    + nonDefaults
                    + " are configured."
            );
        }
    }
}
