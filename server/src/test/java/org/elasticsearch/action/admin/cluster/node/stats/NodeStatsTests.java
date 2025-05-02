/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.NodeStatsLevel;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.coordination.ClusterStateSerializationStats;
import org.elasticsearch.cluster.coordination.PendingClusterStateStats;
import org.elasticsearch.cluster.coordination.PublishClusterStateStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStats;
import org.elasticsearch.cluster.service.ClusterApplierRecordingService;
import org.elasticsearch.cluster.service.ClusterApplierRecordingService.Stats.Recording;
import org.elasticsearch.cluster.service.ClusterStateUpdateStats;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.HttpStatsTests;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.bulk.stats.BulkStats;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.mapper.NodeMappingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardCountStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.SparseVectorStats;
import org.elasticsearch.index.stats.IndexingPressureStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.node.AdaptiveSelectionStats;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.repositories.RepositoriesStats;
import org.elasticsearch.script.ScriptCacheStats;
import org.elasticsearch.script.ScriptContextStats;
import org.elasticsearch.script.ScriptStats;
import org.elasticsearch.script.TimeSeries;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportActionStats;
import org.elasticsearch.transport.TransportStats;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static org.elasticsearch.test.AbstractChunkedSerializingTestCase.assertChunkCount;
import static org.elasticsearch.threadpool.ThreadPoolStatsTests.randomStats;

public class NodeStatsTests extends ESTestCase {
    public void testSerialization() throws IOException {
        NodeStats nodeStats = createNodeStats();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                NodeStats deserializedNodeStats = new NodeStats(in);
                assertEquals(nodeStats.getNode(), deserializedNodeStats.getNode());
                assertEquals(nodeStats.getTimestamp(), deserializedNodeStats.getTimestamp());
                if (nodeStats.getIndices() == null) {
                    assertNull(deserializedNodeStats.getIndices());
                } else {
                    NodeIndicesStats indicesStats = nodeStats.getIndices();
                    NodeIndicesStats deserializedIndicesStats = deserializedNodeStats.getIndices();
                    assertEquals(indicesStats, deserializedIndicesStats);
                }
                if (nodeStats.getOs() == null) {
                    assertNull(deserializedNodeStats.getOs());
                } else {
                    assertEquals(nodeStats.getOs().getTimestamp(), deserializedNodeStats.getOs().getTimestamp());
                    assertEquals(nodeStats.getOs().getSwap().getFree(), deserializedNodeStats.getOs().getSwap().getFree());
                    assertEquals(nodeStats.getOs().getSwap().getTotal(), deserializedNodeStats.getOs().getSwap().getTotal());
                    assertEquals(nodeStats.getOs().getSwap().getUsed(), deserializedNodeStats.getOs().getSwap().getUsed());
                    assertEquals(nodeStats.getOs().getMem().getFree(), deserializedNodeStats.getOs().getMem().getFree());
                    assertEquals(nodeStats.getOs().getMem().getTotal(), deserializedNodeStats.getOs().getMem().getTotal());
                    assertEquals(nodeStats.getOs().getMem().getUsed(), deserializedNodeStats.getOs().getMem().getUsed());
                    assertEquals(nodeStats.getOs().getMem().getFreePercent(), deserializedNodeStats.getOs().getMem().getFreePercent());
                    assertEquals(nodeStats.getOs().getMem().getUsedPercent(), deserializedNodeStats.getOs().getMem().getUsedPercent());
                    assertEquals(nodeStats.getOs().getCpu().getPercent(), deserializedNodeStats.getOs().getCpu().getPercent());
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuAcctControlGroup(),
                        deserializedNodeStats.getOs().getCgroup().getCpuAcctControlGroup()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuAcctUsageNanos(),
                        deserializedNodeStats.getOs().getCgroup().getCpuAcctUsageNanos()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuControlGroup(),
                        deserializedNodeStats.getOs().getCgroup().getCpuControlGroup()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuCfsPeriodMicros(),
                        deserializedNodeStats.getOs().getCgroup().getCpuCfsPeriodMicros()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuCfsQuotaMicros(),
                        deserializedNodeStats.getOs().getCgroup().getCpuCfsQuotaMicros()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuStat().getNumberOfElapsedPeriods(),
                        deserializedNodeStats.getOs().getCgroup().getCpuStat().getNumberOfElapsedPeriods()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuStat().getNumberOfTimesThrottled(),
                        deserializedNodeStats.getOs().getCgroup().getCpuStat().getNumberOfTimesThrottled()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuStat().getTimeThrottledNanos(),
                        deserializedNodeStats.getOs().getCgroup().getCpuStat().getTimeThrottledNanos()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getMemoryLimitInBytes(),
                        deserializedNodeStats.getOs().getCgroup().getMemoryLimitInBytes()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getMemoryUsageInBytes(),
                        deserializedNodeStats.getOs().getCgroup().getMemoryUsageInBytes()
                    );
                    assertArrayEquals(
                        nodeStats.getOs().getCpu().getLoadAverage(),
                        deserializedNodeStats.getOs().getCpu().getLoadAverage(),
                        0
                    );
                }
                if (nodeStats.getProcess() == null) {
                    assertNull(deserializedNodeStats.getProcess());
                } else {
                    assertEquals(nodeStats.getProcess().getTimestamp(), deserializedNodeStats.getProcess().getTimestamp());
                    assertEquals(nodeStats.getProcess().getCpu().getTotal(), deserializedNodeStats.getProcess().getCpu().getTotal());
                    assertEquals(nodeStats.getProcess().getCpu().getPercent(), deserializedNodeStats.getProcess().getCpu().getPercent());
                    assertEquals(
                        nodeStats.getProcess().getMem().getTotalVirtual(),
                        deserializedNodeStats.getProcess().getMem().getTotalVirtual()
                    );
                    assertEquals(
                        nodeStats.getProcess().getMaxFileDescriptors(),
                        deserializedNodeStats.getProcess().getMaxFileDescriptors()
                    );
                    assertEquals(
                        nodeStats.getProcess().getOpenFileDescriptors(),
                        deserializedNodeStats.getProcess().getOpenFileDescriptors()
                    );
                }
                JvmStats jvm = nodeStats.getJvm();
                JvmStats deserializedJvm = deserializedNodeStats.getJvm();
                if (jvm == null) {
                    assertNull(deserializedJvm);
                } else {
                    JvmStats.Mem mem = jvm.getMem();
                    JvmStats.Mem deserializedMem = deserializedJvm.getMem();
                    assertEquals(jvm.getTimestamp(), deserializedJvm.getTimestamp());
                    assertEquals(mem.getHeapUsedPercent(), deserializedMem.getHeapUsedPercent());
                    assertEquals(mem.getHeapUsed(), deserializedMem.getHeapUsed());
                    assertEquals(mem.getHeapCommitted(), deserializedMem.getHeapCommitted());
                    assertEquals(mem.getNonHeapCommitted(), deserializedMem.getNonHeapCommitted());
                    assertEquals(mem.getNonHeapUsed(), deserializedMem.getNonHeapUsed());
                    assertEquals(mem.getHeapMax(), deserializedMem.getHeapMax());
                    JvmStats.Classes classes = jvm.getClasses();
                    assertEquals(classes.getLoadedClassCount(), deserializedJvm.getClasses().getLoadedClassCount());
                    assertEquals(classes.getTotalLoadedClassCount(), deserializedJvm.getClasses().getTotalLoadedClassCount());
                    assertEquals(classes.getUnloadedClassCount(), deserializedJvm.getClasses().getUnloadedClassCount());
                    assertEquals(jvm.getGc().getCollectors().length, deserializedJvm.getGc().getCollectors().length);
                    for (int i = 0; i < jvm.getGc().getCollectors().length; i++) {
                        JvmStats.GarbageCollector garbageCollector = jvm.getGc().getCollectors()[i];
                        JvmStats.GarbageCollector deserializedGarbageCollector = deserializedJvm.getGc().getCollectors()[i];
                        assertEquals(garbageCollector.getName(), deserializedGarbageCollector.getName());
                        assertEquals(garbageCollector.getCollectionCount(), deserializedGarbageCollector.getCollectionCount());
                        assertEquals(garbageCollector.getCollectionTime(), deserializedGarbageCollector.getCollectionTime());
                    }
                    assertEquals(jvm.getThreads().getCount(), deserializedJvm.getThreads().getCount());
                    assertEquals(jvm.getThreads().getPeakCount(), deserializedJvm.getThreads().getPeakCount());
                    assertEquals(jvm.getUptime(), deserializedJvm.getUptime());
                    if (jvm.getBufferPools() == null) {
                        assertNull(deserializedJvm.getBufferPools());
                    } else {
                        assertEquals(jvm.getBufferPools().size(), deserializedJvm.getBufferPools().size());
                        for (int i = 0; i < jvm.getBufferPools().size(); i++) {
                            JvmStats.BufferPool bufferPool = jvm.getBufferPools().get(i);
                            JvmStats.BufferPool deserializedBufferPool = deserializedJvm.getBufferPools().get(i);
                            assertEquals(bufferPool.getName(), deserializedBufferPool.getName());
                            assertEquals(bufferPool.getCount(), deserializedBufferPool.getCount());
                            assertEquals(bufferPool.getTotalCapacity(), deserializedBufferPool.getTotalCapacity());
                            assertEquals(bufferPool.getUsed(), deserializedBufferPool.getUsed());
                        }
                    }
                }
                if (nodeStats.getThreadPool() == null) {
                    assertNull(deserializedNodeStats.getThreadPool());
                } else {
                    assertNotSame(nodeStats.getThreadPool(), deserializedNodeStats.getThreadPool());
                    assertEquals(nodeStats.getThreadPool(), deserializedNodeStats.getThreadPool());
                }

                FsInfo fs = nodeStats.getFs();
                FsInfo deserializedFs = deserializedNodeStats.getFs();
                if (fs == null) {
                    assertNull(deserializedFs);
                } else {
                    assertEquals(fs.getTimestamp(), deserializedFs.getTimestamp());
                    assertEquals(fs.getTotal().getAvailable(), deserializedFs.getTotal().getAvailable());
                    assertEquals(fs.getTotal().getTotal(), deserializedFs.getTotal().getTotal());
                    assertEquals(fs.getTotal().getFree(), deserializedFs.getTotal().getFree());
                    assertEquals(fs.getTotal().getMount(), deserializedFs.getTotal().getMount());
                    assertEquals(fs.getTotal().getPath(), deserializedFs.getTotal().getPath());
                    assertEquals(fs.getTotal().getType(), deserializedFs.getTotal().getType());
                    FsInfo.IoStats ioStats = fs.getIoStats();
                    FsInfo.IoStats deserializedIoStats = deserializedFs.getIoStats();
                    assertEquals(ioStats.getTotalOperations(), deserializedIoStats.getTotalOperations());
                    assertEquals(ioStats.getTotalReadKilobytes(), deserializedIoStats.getTotalReadKilobytes());
                    assertEquals(ioStats.getTotalReadOperations(), deserializedIoStats.getTotalReadOperations());
                    assertEquals(ioStats.getTotalWriteKilobytes(), deserializedIoStats.getTotalWriteKilobytes());
                    assertEquals(ioStats.getTotalWriteOperations(), deserializedIoStats.getTotalWriteOperations());
                    assertEquals(ioStats.getTotalIOTimeMillis(), deserializedIoStats.getTotalIOTimeMillis());
                    assertEquals(ioStats.getDevicesStats().length, deserializedIoStats.getDevicesStats().length);
                    for (int i = 0; i < ioStats.getDevicesStats().length; i++) {
                        FsInfo.DeviceStats deviceStats = ioStats.getDevicesStats()[i];
                        FsInfo.DeviceStats deserializedDeviceStats = deserializedIoStats.getDevicesStats()[i];
                        assertEquals(deviceStats.operations(), deserializedDeviceStats.operations());
                        assertEquals(deviceStats.readKilobytes(), deserializedDeviceStats.readKilobytes());
                        assertEquals(deviceStats.readOperations(), deserializedDeviceStats.readOperations());
                        assertEquals(deviceStats.writeKilobytes(), deserializedDeviceStats.writeKilobytes());
                        assertEquals(deviceStats.writeOperations(), deserializedDeviceStats.writeOperations());
                        assertEquals(deviceStats.ioTimeInMillis(), deserializedDeviceStats.ioTimeInMillis());
                    }
                }
                if (nodeStats.getTransport() == null) {
                    assertNull(deserializedNodeStats.getTransport());
                } else {
                    assertEquals(nodeStats.getTransport().getRxCount(), deserializedNodeStats.getTransport().getRxCount());
                    assertEquals(nodeStats.getTransport().getRxSize(), deserializedNodeStats.getTransport().getRxSize());
                    assertEquals(nodeStats.getTransport().getServerOpen(), deserializedNodeStats.getTransport().getServerOpen());
                    assertEquals(nodeStats.getTransport().getTxCount(), deserializedNodeStats.getTransport().getTxCount());
                    assertEquals(nodeStats.getTransport().getTxSize(), deserializedNodeStats.getTransport().getTxSize());
                    assertArrayEquals(
                        nodeStats.getTransport().getInboundHandlingTimeBucketFrequencies(),
                        deserializedNodeStats.getTransport().getInboundHandlingTimeBucketFrequencies()
                    );
                    assertArrayEquals(
                        nodeStats.getTransport().getOutboundHandlingTimeBucketFrequencies(),
                        deserializedNodeStats.getTransport().getOutboundHandlingTimeBucketFrequencies()
                    );
                }

                assertEquals(nodeStats.getHttp(), deserializedNodeStats.getHttp());

                if (nodeStats.getBreaker() == null) {
                    assertNull(deserializedNodeStats.getBreaker());
                } else {
                    assertEquals(nodeStats.getBreaker().getAllStats().length, deserializedNodeStats.getBreaker().getAllStats().length);
                    for (int i = 0; i < nodeStats.getBreaker().getAllStats().length; i++) {
                        CircuitBreakerStats circuitBreakerStats = nodeStats.getBreaker().getAllStats()[i];
                        CircuitBreakerStats deserializedCircuitBreakerStats = deserializedNodeStats.getBreaker().getAllStats()[i];
                        assertEquals(circuitBreakerStats.getEstimated(), deserializedCircuitBreakerStats.getEstimated());
                        assertEquals(circuitBreakerStats.getLimit(), deserializedCircuitBreakerStats.getLimit());
                        assertEquals(circuitBreakerStats.getName(), deserializedCircuitBreakerStats.getName());
                        assertEquals(circuitBreakerStats.getOverhead(), deserializedCircuitBreakerStats.getOverhead(), 0);
                        assertEquals(circuitBreakerStats.getTrippedCount(), deserializedCircuitBreakerStats.getTrippedCount(), 0);
                    }
                }
                ScriptStats scriptStats = nodeStats.getScriptStats();
                ScriptStats deserializedScriptStats = deserializedNodeStats.getScriptStats();
                if (scriptStats == null) {
                    assertNull(deserializedScriptStats);
                } else {
                    assertEquals(scriptStats, deserializedScriptStats);
                    assertNotSame(scriptStats, deserializedScriptStats);
                }
                DiscoveryStats discoveryStats = nodeStats.getDiscoveryStats();
                DiscoveryStats deserializedDiscoveryStats = deserializedNodeStats.getDiscoveryStats();
                if (discoveryStats == null) {
                    assertNull(deserializedDiscoveryStats);
                } else {
                    PendingClusterStateStats queueStats = discoveryStats.getQueueStats();
                    if (queueStats == null) {
                        assertNull(deserializedDiscoveryStats.getQueueStats());
                    } else {
                        assertEquals(queueStats.getCommitted(), deserializedDiscoveryStats.getQueueStats().getCommitted());
                        assertEquals(queueStats.getTotal(), deserializedDiscoveryStats.getQueueStats().getTotal());
                        assertEquals(queueStats.getPending(), deserializedDiscoveryStats.getQueueStats().getPending());
                    }

                    final PublishClusterStateStats publishStats = discoveryStats.getPublishStats();
                    if (publishStats == null) {
                        assertNull(deserializedDiscoveryStats.getPublishStats());
                    } else {
                        final PublishClusterStateStats deserializedPublishStats = deserializedDiscoveryStats.getPublishStats();
                        assertEquals(
                            publishStats.getFullClusterStateReceivedCount(),
                            deserializedPublishStats.getFullClusterStateReceivedCount()
                        );
                        assertEquals(
                            publishStats.getCompatibleClusterStateDiffReceivedCount(),
                            deserializedPublishStats.getCompatibleClusterStateDiffReceivedCount()
                        );
                        assertEquals(
                            publishStats.getIncompatibleClusterStateDiffReceivedCount(),
                            deserializedPublishStats.getIncompatibleClusterStateDiffReceivedCount()
                        );
                    }

                    final ClusterStateUpdateStats clusterStateUpdateStats = discoveryStats.getClusterStateUpdateStats();
                    if (clusterStateUpdateStats == null) {
                        assertNull(deserializedDiscoveryStats.getClusterStateUpdateStats());
                    } else {
                        final ClusterStateUpdateStats deserializedClusterStateUpdateStats = deserializedDiscoveryStats
                            .getClusterStateUpdateStats();
                        assertEquals(
                            clusterStateUpdateStats.getUnchangedTaskCount(),
                            deserializedClusterStateUpdateStats.getUnchangedTaskCount()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getPublicationSuccessCount(),
                            deserializedClusterStateUpdateStats.getPublicationSuccessCount()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getPublicationFailureCount(),
                            deserializedClusterStateUpdateStats.getPublicationFailureCount()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getUnchangedComputationElapsedMillis(),
                            deserializedClusterStateUpdateStats.getUnchangedComputationElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getUnchangedNotificationElapsedMillis(),
                            deserializedClusterStateUpdateStats.getUnchangedNotificationElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getSuccessfulComputationElapsedMillis(),
                            deserializedClusterStateUpdateStats.getSuccessfulComputationElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getSuccessfulPublicationElapsedMillis(),
                            deserializedClusterStateUpdateStats.getSuccessfulPublicationElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getSuccessfulContextConstructionElapsedMillis(),
                            deserializedClusterStateUpdateStats.getSuccessfulContextConstructionElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getSuccessfulCommitElapsedMillis(),
                            deserializedClusterStateUpdateStats.getSuccessfulCommitElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getSuccessfulCompletionElapsedMillis(),
                            deserializedClusterStateUpdateStats.getSuccessfulCompletionElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getSuccessfulMasterApplyElapsedMillis(),
                            deserializedClusterStateUpdateStats.getSuccessfulMasterApplyElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getSuccessfulNotificationElapsedMillis(),
                            deserializedClusterStateUpdateStats.getSuccessfulNotificationElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getFailedComputationElapsedMillis(),
                            deserializedClusterStateUpdateStats.getFailedComputationElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getFailedPublicationElapsedMillis(),
                            deserializedClusterStateUpdateStats.getFailedPublicationElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getFailedContextConstructionElapsedMillis(),
                            deserializedClusterStateUpdateStats.getFailedContextConstructionElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getFailedCommitElapsedMillis(),
                            deserializedClusterStateUpdateStats.getFailedCommitElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getFailedCompletionElapsedMillis(),
                            deserializedClusterStateUpdateStats.getFailedCompletionElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getFailedMasterApplyElapsedMillis(),
                            deserializedClusterStateUpdateStats.getFailedMasterApplyElapsedMillis()
                        );
                        assertEquals(
                            clusterStateUpdateStats.getFailedNotificationElapsedMillis(),
                            deserializedClusterStateUpdateStats.getFailedNotificationElapsedMillis()
                        );
                    }
                }
                IngestStats ingestStats = nodeStats.getIngestStats();
                IngestStats deserializedIngestStats = deserializedNodeStats.getIngestStats();
                if (ingestStats == null) {
                    assertNull(deserializedIngestStats);
                } else {
                    assertNotSame(ingestStats, deserializedIngestStats);
                    assertEquals(ingestStats, deserializedIngestStats);
                }
                AdaptiveSelectionStats adaptiveStats = nodeStats.getAdaptiveSelectionStats();
                AdaptiveSelectionStats deserializedAdaptiveStats = deserializedNodeStats.getAdaptiveSelectionStats();
                if (adaptiveStats == null) {
                    assertNull(deserializedAdaptiveStats);
                } else {
                    assertEquals(adaptiveStats.getOutgoingConnections(), deserializedAdaptiveStats.getOutgoingConnections());
                    assertEquals(adaptiveStats.getRanks(), deserializedAdaptiveStats.getRanks());
                    adaptiveStats.getComputedStats().forEach((k, v) -> {
                        ResponseCollectorService.ComputedNodeStats aStats = adaptiveStats.getComputedStats().get(k);
                        ResponseCollectorService.ComputedNodeStats bStats = deserializedAdaptiveStats.getComputedStats().get(k);
                        assertEquals(aStats.nodeId, bStats.nodeId);
                        assertEquals(aStats.queueSize, bStats.queueSize, 0.01);
                        assertEquals(aStats.serviceTime, bStats.serviceTime, 0.01);
                        assertEquals(aStats.responseTime, bStats.responseTime, 0.01);
                    });
                }
                var scriptCacheStats = nodeStats.getScriptCacheStats();
                var deserializedScriptCacheStats = deserializedNodeStats.getScriptCacheStats();
                if (scriptCacheStats == null) {
                    assertNull(deserializedScriptCacheStats);
                } else if (deserializedScriptCacheStats.getContextStats() != null) {
                    assertEquals(scriptCacheStats, deserializedScriptCacheStats);
                    assertNotSame(scriptCacheStats, deserializedScriptCacheStats);
                }

                RepositoriesStats repoThrottlingStats = deserializedNodeStats.getRepositoriesStats();
                assertTrue(repoThrottlingStats.getRepositoryThrottlingStats().containsKey("test-repository"));
                assertEquals(100, repoThrottlingStats.getRepositoryThrottlingStats().get("test-repository").totalReadThrottledNanos());
                assertEquals(200, repoThrottlingStats.getRepositoryThrottlingStats().get("test-repository").totalWriteThrottledNanos());

            }
        }
    }

    public void testChunking() {
        assertChunkCount(createNodeStats(), ToXContent.EMPTY_PARAMS, nodeStats -> expectedChunks(nodeStats, ToXContent.EMPTY_PARAMS));
        for (NodeStatsLevel l : NodeStatsLevel.values()) {
            ToXContent.Params p = new ToXContent.MapParams(Map.of("level", l.getLevel()));
            assertChunkCount(createNodeStats(), p, nodeStats -> expectedChunks(nodeStats, p));
        }
    }

    private static int expectedChunks(NodeStats nodeStats, ToXContent.Params params) {
        return 3 // number of static chunks, see NodeStats#toXContentChunked
            + assertExpectedChunks(nodeStats.getIndices(), i -> expectedChunks(i, NodeStatsLevel.of(params, NodeStatsLevel.NODE)), params)
            + assertExpectedChunks(nodeStats.getThreadPool(), NodeStatsTests::expectedChunks, params) // <br/>
            + chunkIfPresent(nodeStats.getFs()) // <br/>
            + assertExpectedChunks(nodeStats.getTransport(), NodeStatsTests::expectedChunks, params) // <br/>
            + assertExpectedChunks(nodeStats.getHttp(), NodeStatsTests::expectedChunks, params) // <br/>
            + chunkIfPresent(nodeStats.getBreaker()) // <br/>
            + assertExpectedChunks(nodeStats.getScriptStats(), NodeStatsTests::expectedChunks, params) // <br/>
            + chunkIfPresent(nodeStats.getDiscoveryStats()) // <br/>
            + assertExpectedChunks(nodeStats.getIngestStats(), NodeStatsTests::expectedChunks, params) // <br/>
            + chunkIfPresent(nodeStats.getAdaptiveSelectionStats()) // <br/>
            + chunkIfPresent(nodeStats.getScriptCacheStats());
    }

    private static int chunkIfPresent(ToXContent xcontent) {
        return xcontent == null ? 0 : 1;
    }

    private static <T extends ChunkedToXContent> int assertExpectedChunks(T obj, ToIntFunction<T> getChunks, ToXContent.Params params) {
        if (obj == null) return 0;
        int chunks = getChunks.applyAsInt(obj);
        assertChunkCount(obj, params, t -> chunks);
        return chunks;
    }

    private static int expectedChunks(ScriptStats scriptStats) {
        return 4 + (scriptStats.compilationsHistory() != null && scriptStats.compilationsHistory().areTimingsEmpty() == false ? 1 : 0)
            + (scriptStats.cacheEvictionsHistory() != null && scriptStats.cacheEvictionsHistory().areTimingsEmpty() == false ? 1 : 0)
            + scriptStats.contextStats().size();
    }

    private static int expectedChunks(ThreadPoolStats threadPool) {
        return 2 + threadPool.stats().stream().mapToInt(s -> {
            var chunks = 0;
            chunks += s.threads() == -1 ? 0 : 1;
            chunks += s.queue() == -1 ? 0 : 1;
            chunks += s.active() == -1 ? 0 : 1;
            chunks += s.rejected() == -1 ? 0 : 1;
            chunks += s.largest() == -1 ? 0 : 1;
            chunks += s.completed() == -1 ? 0 : 1;
            return 2 + chunks; // start + endObject + chunks
        }).sum();
    }

    private static int expectedChunks(IngestStats ingestStats) {
        return 2 + ingestStats.pipelineStats()
            .stream()
            .mapToInt(pipelineStats -> 2 + ingestStats.processorStats().getOrDefault(pipelineStats.pipelineId(), List.of()).size())
            .sum();
    }

    private static int expectedChunks(HttpStats httpStats) {
        return 3 + httpStats.getClientStats().size() + httpStats.httpRouteStats().size();
    }

    private static int expectedChunks(TransportStats transportStats) {
        return 3; // only one transport action
    }

    private static int expectedChunks(NodeIndicesStats nodeIndicesStats, NodeStatsLevel level) {
        return nodeIndicesStats == null ? 0 : switch (level) {
            case NODE -> 2;
            case INDICES -> 5; // only one index
            case SHARDS -> 9; // only one shard
        };
    }

    private static CommonStats createIndexLevelCommonStats() {
        CommonStats stats = new CommonStats(new CommonStatsFlags().clear().set(CommonStatsFlags.Flag.Mappings, true));
        stats.nodeMappings = new NodeMappingStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        return stats;
    }

    private static CommonStats createShardLevelCommonStats() {
        int iota = 0;

        final CommonStats indicesCommonStats = new CommonStats(CommonStatsFlags.ALL);
        indicesCommonStats.getDocs().add(new DocsStats(++iota, ++iota, ++iota));
        Map<String, FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats> fieldOrdinalStats = new HashMap<>();
        fieldOrdinalStats.put(
            randomAlphaOfLength(4),
            new FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats(randomNonNegativeLong(), randomNonNegativeLong())
        );
        var ordinalStats = new FieldDataStats.GlobalOrdinalsStats(randomNonNegativeLong(), fieldOrdinalStats);
        indicesCommonStats.getFieldData().add(new FieldDataStats(++iota, ++iota, null, ordinalStats));
        indicesCommonStats.getStore().add(new StoreStats(++iota, ++iota, ++iota));

        final IndexingStats.Stats indexingStats = new IndexingStats.Stats(
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            false,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota
        );
        indicesCommonStats.getIndexing().add(new IndexingStats(indexingStats));
        indicesCommonStats.getQueryCache().add(new QueryCacheStats(++iota, ++iota, ++iota, ++iota, ++iota));
        indicesCommonStats.getRequestCache().add(new RequestCacheStats(++iota, ++iota, ++iota, ++iota));

        final SearchStats.Stats searchStats = new SearchStats.Stats(
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota,
            ++iota
        );
        Map<String, SearchStats.Stats> groupStats = new HashMap<>();
        groupStats.put("group", searchStats);
        indicesCommonStats.getSearch().add(new SearchStats(searchStats, ++iota, groupStats));

        final SegmentsStats segmentsStats = new SegmentsStats();
        segmentsStats.add(++iota);
        segmentsStats.addIndexWriterMemoryInBytes(++iota);
        segmentsStats.addVersionMapMemoryInBytes(++iota);
        segmentsStats.addBitsetMemoryInBytes(++iota);
        indicesCommonStats.getSegments().add(segmentsStats);

        indicesCommonStats.getGet().add(new GetStats(++iota, ++iota, ++iota, ++iota, ++iota));

        MergeStats mergeStats = new MergeStats();
        mergeStats.add(++iota, ++iota, ++iota, ++iota, ++iota, ++iota, ++iota, ++iota, ++iota, 1.0 * ++iota);

        indicesCommonStats.getMerge().add(mergeStats);
        indicesCommonStats.getRefresh().add(new RefreshStats(++iota, ++iota, ++iota, ++iota, ++iota));
        indicesCommonStats.getFlush().add(new FlushStats(++iota, ++iota, ++iota, ++iota));
        indicesCommonStats.getWarmer().add(new WarmerStats(++iota, ++iota, ++iota));
        indicesCommonStats.getCompletion().add(new CompletionStats(++iota, null));
        indicesCommonStats.getTranslog().add(new TranslogStats(++iota, ++iota, ++iota, ++iota, ++iota));

        RecoveryStats recoveryStats = new RecoveryStats();
        recoveryStats.incCurrentAsSource();
        recoveryStats.incCurrentAsTarget();
        recoveryStats.addThrottleTime(++iota);
        indicesCommonStats.getRecoveryStats().add(recoveryStats);

        indicesCommonStats.getBulk().add(new BulkStats(++iota, ++iota, ++iota, ++iota, ++iota));
        indicesCommonStats.getShards().add(new ShardCountStats(++iota));

        indicesCommonStats.getDenseVectorStats().add(new DenseVectorStats(++iota));
        indicesCommonStats.getSparseVectorStats().add(new SparseVectorStats(++iota));

        return indicesCommonStats;
    }

    private static ShardStats createShardStats(ShardId shardId) {
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "message"),
            ShardRouting.Role.DEFAULT
        );
        Path path = createTempDir().resolve("indices")
            .resolve(shardRouting.shardId().getIndex().getUUID())
            .resolve(String.valueOf(shardRouting.shardId().id()));
        ShardPath shardPath = new ShardPath(false, path, path, shardRouting.shardId());
        return new ShardStats(shardRouting, shardPath, createShardLevelCommonStats(), null, null, null, false, 0);
    }

    public static NodeStats createNodeStats() {
        DiscoveryNode node = DiscoveryNodeUtils.builder("test_node")
            .roles(emptySet())
            .version(VersionUtils.randomVersion(random()), IndexVersions.ZERO, IndexVersionUtils.randomVersion())
            .build();
        NodeIndicesStats nodeIndicesStats = null;
        if (frequently()) {
            final Index indexTest = new Index("test", "_na_");
            Map<Index, CommonStats> statsByIndex = new HashMap<>();
            statsByIndex.put(indexTest, createIndexLevelCommonStats());

            ShardId shardId = new ShardId(indexTest, 0);
            ShardStats shardStat = createShardStats(shardId);
            IndexShardStats shardStats = new IndexShardStats(shardId, new ShardStats[] { shardStat });
            Map<Index, List<IndexShardStats>> statsByShard = new HashMap<>();
            List<IndexShardStats> indexShardStats = new ArrayList<>();
            indexShardStats.add(shardStats);
            statsByShard.put(indexTest, indexShardStats);

            CommonStats oldStats = new CommonStats(CommonStatsFlags.ALL);
            nodeIndicesStats = new NodeIndicesStats(oldStats, statsByIndex, statsByShard, true);
        }
        OsStats osStats = null;
        if (frequently()) {
            double loadAverages[] = new double[3];
            for (int i = 0; i < 3; i++) {
                loadAverages[i] = randomBoolean() ? randomDouble() : -1;
            }
            long memTotal = randomNonNegativeLong();
            long swapTotal = randomNonNegativeLong();
            osStats = new OsStats(
                System.currentTimeMillis(),
                new OsStats.Cpu(randomShort(), loadAverages),
                new OsStats.Mem(memTotal, randomLongBetween(0, memTotal), randomLongBetween(0, memTotal)),
                new OsStats.Swap(swapTotal, randomLongBetween(0, swapTotal)),
                new OsStats.Cgroup(
                    randomAlphaOfLength(8),
                    randomUnsignedLongBetween(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TWO)),
                    randomAlphaOfLength(8),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    new OsStats.Cgroup.CpuStat(
                        randomUnsignedLongBetween(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TWO)),
                        randomUnsignedLongBetween(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TWO)),
                        randomUnsignedLongBetween(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TWO))
                    ),
                    randomAlphaOfLength(8),
                    Long.toString(randomNonNegativeLong()),
                    Long.toString(randomNonNegativeLong())
                )
            );
        }
        ProcessStats processStats = frequently()
            ? new ProcessStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                new ProcessStats.Cpu(randomShort(), randomNonNegativeLong()),
                new ProcessStats.Mem(randomNonNegativeLong())
            )
            : null;
        JvmStats jvmStats = null;
        if (frequently()) {
            int numMemoryPools = randomIntBetween(0, 10);
            List<JvmStats.MemoryPool> memoryPools = new ArrayList<>(numMemoryPools);
            for (int i = 0; i < numMemoryPools; i++) {
                memoryPools.add(
                    new JvmStats.MemoryPool(
                        randomAlphaOfLengthBetween(3, 10),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    )
                );
            }
            JvmStats.Threads threads = new JvmStats.Threads(randomIntBetween(1, 1000), randomIntBetween(1, 1000));
            int numGarbageCollectors = randomIntBetween(0, 10);
            JvmStats.GarbageCollector[] garbageCollectorsArray = new JvmStats.GarbageCollector[numGarbageCollectors];
            for (int i = 0; i < numGarbageCollectors; i++) {
                garbageCollectorsArray[i] = new JvmStats.GarbageCollector(
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                );
            }
            JvmStats.GarbageCollectors garbageCollectors = new JvmStats.GarbageCollectors(garbageCollectorsArray);
            int numBufferPools = randomIntBetween(0, 10);
            List<JvmStats.BufferPool> bufferPoolList = new ArrayList<>();
            for (int i = 0; i < numBufferPools; i++) {
                bufferPoolList.add(
                    new JvmStats.BufferPool(
                        randomAlphaOfLengthBetween(3, 10),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    )
                );
            }
            JvmStats.Classes classes = new JvmStats.Classes(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
            jvmStats = frequently()
                ? new JvmStats(
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    new JvmStats.Mem(
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        memoryPools
                    ),
                    threads,
                    garbageCollectors,
                    randomBoolean() ? Collections.emptyList() : bufferPoolList,
                    classes
                )
                : null;
        }
        ThreadPoolStats threadPoolStats = null;
        if (frequently()) {
            threadPoolStats = new ThreadPoolStats(
                IntStream.range(0, randomIntBetween(0, 10)).mapToObj(i -> randomStats(randomAlphaOfLengthBetween(3, 10))).toList()
            );
        }
        FsInfo fsInfo = null;
        if (frequently()) {
            int numDeviceStats = randomIntBetween(0, 10);
            FsInfo.DeviceStats[] deviceStatsArray = new FsInfo.DeviceStats[numDeviceStats];
            for (int i = 0; i < numDeviceStats; i++) {
                FsInfo.DeviceStats previousDeviceStats = randomBoolean()
                    ? null
                    : new FsInfo.DeviceStats(
                        randomInt(),
                        randomInt(),
                        randomAlphaOfLengthBetween(3, 10),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        null
                    );
                deviceStatsArray[i] = new FsInfo.DeviceStats(
                    randomInt(),
                    randomInt(),
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    previousDeviceStats
                );
            }
            FsInfo.IoStats ioStats = new FsInfo.IoStats(deviceStatsArray);
            int numPaths = randomIntBetween(0, 10);
            FsInfo.Path[] paths = new FsInfo.Path[numPaths];
            for (int i = 0; i < numPaths; i++) {
                paths[i] = new FsInfo.Path(
                    randomAlphaOfLengthBetween(3, 10),
                    randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null,
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                );
            }
            fsInfo = new FsInfo(randomNonNegativeLong(), ioStats, paths);
        }
        TransportStats transportStats = frequently()
            ? new TransportStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                IntStream.range(0, HandlingTimeTracker.BUCKET_COUNT).mapToLong(i -> randomNonNegativeLong()).toArray(),
                IntStream.range(0, HandlingTimeTracker.BUCKET_COUNT).mapToLong(i -> randomNonNegativeLong()).toArray(),
                Map.of("test-action", new TransportActionStats(1, 2, new long[29], 3, 4, new long[29]))
            )
            : null;
        HttpStats httpStats = null;
        if (frequently()) {
            int numClients = randomIntBetween(0, 50);
            List<HttpStats.ClientStats> clientStats = new ArrayList<>(numClients);
            for (int k = 0; k < numClients; k++) {
                var cs = new HttpStats.ClientStats(
                    randomInt(),
                    randomAlphaOfLength(6),
                    randomAlphaOfLength(6),
                    randomAlphaOfLength(6),
                    randomAlphaOfLength(6),
                    randomAlphaOfLength(6),
                    randomAlphaOfLength(6),
                    randomNonNegativeLong(),
                    randomBoolean() ? -1 : randomNonNegativeLong(),
                    randomBoolean() ? -1 : randomNonNegativeLong(),
                    randomLongBetween(0, 100),
                    randomLongBetween(0, 99999999)
                );
                clientStats.add(cs);
            }
            httpStats = new HttpStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                clientStats,
                randomMap(1, 3, () -> new Tuple<>(randomAlphaOfLength(10), HttpStatsTests.randomHttpRouteStats()))
            );
        }
        AllCircuitBreakerStats allCircuitBreakerStats = null;
        if (frequently()) {
            int numCircuitBreakerStats = randomIntBetween(0, 10);
            CircuitBreakerStats[] circuitBreakerStatsArray = new CircuitBreakerStats[numCircuitBreakerStats];
            for (int i = 0; i < numCircuitBreakerStats; i++) {
                circuitBreakerStatsArray[i] = new CircuitBreakerStats(
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomDouble(),
                    randomNonNegativeLong()
                );
            }
            allCircuitBreakerStats = new AllCircuitBreakerStats(circuitBreakerStatsArray);
        }
        ScriptStats scriptStats = null;
        if (frequently()) {
            int numContents = randomIntBetween(0, 20);
            List<ScriptContextStats> stats = new ArrayList<>(numContents);
            HashSet<String> contexts = new HashSet<>();
            for (int i = 0; i < numContents; i++) {
                String context = randomValueOtherThanMany(contexts::contains, () -> randomAlphaOfLength(12));
                contexts.add(context);
                stats.add(new ScriptContextStats(context, randomLongBetween(0, 1024), randomTimeSeries(), randomTimeSeries()));
            }
            scriptStats = ScriptStats.read(stats);
        }
        ClusterApplierRecordingService.Stats timeTrackerStats;
        if (randomBoolean()) {
            timeTrackerStats = new ClusterApplierRecordingService.Stats(
                randomMap(2, 32, () -> new Tuple<>(randomAlphaOfLength(4), new Recording(randomNonNegativeLong(), randomNonNegativeLong())))
            );
        } else {
            timeTrackerStats = null;
        }

        DiscoveryStats discoveryStats = frequently()
            ? new DiscoveryStats(
                randomBoolean() ? new PendingClusterStateStats(randomInt(), randomInt(), randomInt()) : null,
                randomBoolean()
                    ? new PublishClusterStateStats(
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        new ClusterStateSerializationStats(
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong()
                        )
                    )
                    : null,
                randomBoolean()
                    ? new ClusterStateUpdateStats(
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    )
                    : null,
                timeTrackerStats
            )
            : null;
        IngestStats ingestStats = null;
        if (frequently()) {
            int numPipelines = randomIntBetween(0, 10);
            int numProcessors = randomIntBetween(0, 10);
            long maxStatValue = Long.MAX_VALUE / Math.max(1, numPipelines) / Math.max(1, numProcessors);
            IngestStats.Stats totalStats = new IngestStats.Stats(
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue)
            );
            List<IngestStats.PipelineStat> ingestPipelineStats = new ArrayList<>(numPipelines);
            Map<String, List<IngestStats.ProcessorStat>> ingestProcessorStats = Maps.newMapWithExpectedSize(numPipelines);
            for (int i = 0; i < numPipelines; i++) {
                String pipelineId = randomAlphaOfLengthBetween(3, 10);
                ingestPipelineStats.add(
                    new IngestStats.PipelineStat(
                        pipelineId,
                        new IngestStats.Stats(
                            randomLongBetween(0, maxStatValue),
                            randomLongBetween(0, maxStatValue),
                            randomLongBetween(0, maxStatValue),
                            randomLongBetween(0, maxStatValue)
                        ),
                        new IngestStats.ByteStats(randomLongBetween(0, maxStatValue), randomLongBetween(0, maxStatValue))
                    )
                );

                List<IngestStats.ProcessorStat> processorPerPipeline = new ArrayList<>(numProcessors);
                for (int j = 0; j < numProcessors; j++) {
                    IngestStats.Stats processorStats = new IngestStats.Stats(
                        randomLongBetween(0, maxStatValue),
                        randomLongBetween(0, maxStatValue),
                        randomLongBetween(0, maxStatValue),
                        randomLongBetween(0, maxStatValue)
                    );
                    processorPerPipeline.add(
                        new IngestStats.ProcessorStat(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), processorStats)
                    );
                }
                ingestProcessorStats.put(pipelineId, processorPerPipeline);
            }
            ingestStats = new IngestStats(totalStats, ingestPipelineStats, ingestProcessorStats);
        }
        AdaptiveSelectionStats adaptiveSelectionStats = null;
        if (frequently()) {
            int numNodes = randomIntBetween(0, 10);
            Map<String, Long> nodeConnections = new HashMap<>();
            Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = new HashMap<>();
            for (int i = 0; i < numNodes; i++) {
                String nodeId = randomAlphaOfLengthBetween(3, 10);
                // add outgoing connection info
                if (frequently()) {
                    nodeConnections.put(nodeId, randomLongBetween(0, 100));
                }
                // add node calculations
                if (frequently()) {
                    ResponseCollectorService.ComputedNodeStats stats = new ResponseCollectorService.ComputedNodeStats(
                        nodeId,
                        randomIntBetween(1, 10),
                        randomIntBetween(0, 2000),
                        randomDoubleBetween(1.0, 10000000.0, true),
                        randomDoubleBetween(1.0, 10000000.0, true)
                    );
                    nodeStats.put(nodeId, stats);
                }
            }
            adaptiveSelectionStats = new AdaptiveSelectionStats(nodeConnections, nodeStats);
        }
        ScriptCacheStats scriptCacheStats = scriptStats != null ? scriptStats.toScriptCacheStats() : null;
        IndexingPressureStats indexingPressureStats = null;
        if (frequently()) {
            long maxStatValue = Long.MAX_VALUE / 5;
            indexingPressureStats = new IndexingPressureStats(
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue),
                randomLongBetween(0, maxStatValue)
            );
        }
        RepositoriesStats repositoriesStats = new RepositoriesStats(
            Map.of("test-repository", new RepositoriesStats.ThrottlingStats(100, 200))
        );
        NodeAllocationStats nodeAllocationStats = new NodeAllocationStats(
            randomIntBetween(0, 10000),
            randomIntBetween(0, 1000),
            randomDoubleBetween(0, 8, true),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );

        return new NodeStats(
            node,
            randomNonNegativeLong(),
            nodeIndicesStats,
            osStats,
            processStats,
            jvmStats,
            threadPoolStats,
            fsInfo,
            transportStats,
            httpStats,
            allCircuitBreakerStats,
            scriptStats,
            discoveryStats,
            ingestStats,
            adaptiveSelectionStats,
            scriptCacheStats,
            indexingPressureStats,
            repositoriesStats,
            nodeAllocationStats
        );
    }

    private static TimeSeries randomTimeSeries() {
        if (randomBoolean()) {
            long total = randomLongBetween(0, 1024);
            long day = total >= 1 ? randomLongBetween(0, total) : 0;
            long fifteen = day >= 1 ? randomLongBetween(0, day) : 0;
            long five = fifteen >= 1 ? randomLongBetween(0, fifteen) : 0;
            return new TimeSeries(five, fifteen, day, day);
        } else {
            return new TimeSeries(randomLongBetween(0, 1024));
        }
    }

}
