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

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.zen.publish.PendingClusterStateStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.script.ScriptStats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class NodeStatsTests extends ESTestCase {

    public void testSerialization() throws IOException {
        NodeStats nodeStats = createNodeStats();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                NodeStats deserializedNodeStats = NodeStats.readNodeStats(in);
                assertEquals(nodeStats.getNode(), deserializedNodeStats.getNode());
                assertEquals(nodeStats.getTimestamp(), deserializedNodeStats.getTimestamp());
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
                    assertArrayEquals(nodeStats.getOs().getCpu().getLoadAverage(),
                            deserializedNodeStats.getOs().getCpu().getLoadAverage(), 0);
                }
                if (nodeStats.getProcess() == null) {
                    assertNull(deserializedNodeStats.getProcess());
                } else {
                    assertEquals(nodeStats.getProcess().getTimestamp(), deserializedNodeStats.getProcess().getTimestamp());
                    assertEquals(nodeStats.getProcess().getCpu().getTotal(), deserializedNodeStats.getProcess().getCpu().getTotal());
                    assertEquals(nodeStats.getProcess().getCpu().getPercent(), deserializedNodeStats.getProcess().getCpu().getPercent());
                    assertEquals(nodeStats.getProcess().getMem().getTotalVirtual(),
                            deserializedNodeStats.getProcess().getMem().getTotalVirtual());
                    assertEquals(nodeStats.getProcess().getMaxFileDescriptors(),
                            deserializedNodeStats.getProcess().getMaxFileDescriptors());
                    assertEquals(nodeStats.getProcess().getOpenFileDescriptors(),
                            deserializedNodeStats.getProcess().getOpenFileDescriptors());
                }
                if (nodeStats.getJvm() == null) {
                    assertNull(deserializedNodeStats.getJvm());
                } else {
                    assertEquals(nodeStats.getJvm().getTimestamp(), deserializedNodeStats.getJvm().getTimestamp());
                    assertEquals(nodeStats.getJvm().getMem().getHeapUsedPercent(),
                            deserializedNodeStats.getJvm().getMem().getHeapUsedPercent());
                    assertEquals(nodeStats.getJvm().getMem().getHeapUsed(), deserializedNodeStats.getJvm().getMem().getHeapUsed());
                    assertEquals(nodeStats.getJvm().getMem().getHeapCommitted(),
                            deserializedNodeStats.getJvm().getMem().getHeapCommitted());
                    assertEquals(nodeStats.getJvm().getMem().getNonHeapCommitted(),
                            deserializedNodeStats.getJvm().getMem().getNonHeapCommitted());
                    assertEquals(nodeStats.getJvm().getMem().getNonHeapUsed(), deserializedNodeStats.getJvm().getMem().getNonHeapUsed());
                    assertEquals(nodeStats.getJvm().getMem().getHeapMax(), deserializedNodeStats.getJvm().getMem().getHeapMax());
                    assertEquals(nodeStats.getJvm().getClasses().getLoadedClassCount(),
                            deserializedNodeStats.getJvm().getClasses().getLoadedClassCount());
                    assertEquals(nodeStats.getJvm().getClasses().getTotalLoadedClassCount(),
                            deserializedNodeStats.getJvm().getClasses().getTotalLoadedClassCount());
                    assertEquals(nodeStats.getJvm().getClasses().getUnloadedClassCount(),
                            deserializedNodeStats.getJvm().getClasses().getUnloadedClassCount());
                    assertEquals(nodeStats.getJvm().getGc().getCollectors().length,
                            deserializedNodeStats.getJvm().getGc().getCollectors().length);
                    for (int i = 0; i < nodeStats.getJvm().getGc().getCollectors().length; i++) {
                        JvmStats.GarbageCollector garbageCollector = nodeStats.getJvm().getGc().getCollectors()[i];
                        JvmStats.GarbageCollector deserializedGarbageCollector = deserializedNodeStats.getJvm().getGc().getCollectors()[i];
                        assertEquals(garbageCollector.getName(), deserializedGarbageCollector.getName());
                        assertEquals(garbageCollector.getCollectionCount(), deserializedGarbageCollector.getCollectionCount());
                        assertEquals(garbageCollector.getCollectionTime(), deserializedGarbageCollector.getCollectionTime());
                    }
                    assertEquals(nodeStats.getJvm().getThreads().getCount(), deserializedNodeStats.getJvm().getThreads().getCount());
                    assertEquals(nodeStats.getJvm().getThreads().getPeakCount(),
                            deserializedNodeStats.getJvm().getThreads().getPeakCount());
                    assertEquals(nodeStats.getJvm().getUptime(), deserializedNodeStats.getJvm().getUptime());
                    if (nodeStats.getJvm().getBufferPools() == null) {
                        assertNull(deserializedNodeStats.getJvm().getBufferPools());
                    } else {
                        assertEquals(nodeStats.getJvm().getBufferPools().size(), deserializedNodeStats.getJvm().getBufferPools().size());
                        for (int i = 0; i < nodeStats.getJvm().getBufferPools().size(); i++) {
                            JvmStats.BufferPool bufferPool = nodeStats.getJvm().getBufferPools().get(i);
                            JvmStats.BufferPool deserializedBufferPool = deserializedNodeStats.getJvm().getBufferPools().get(i);
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
                    Iterator<ThreadPoolStats.Stats> threadPoolIterator = nodeStats.getThreadPool().iterator();
                    Iterator<ThreadPoolStats.Stats> deserializedThreadPoolIterator = deserializedNodeStats.getThreadPool().iterator();
                    while (threadPoolIterator.hasNext()) {
                        ThreadPoolStats.Stats stats = threadPoolIterator.next();
                        ThreadPoolStats.Stats deserializedStats = deserializedThreadPoolIterator.next();
                        assertEquals(stats.getName(), deserializedStats.getName());
                        assertEquals(stats.getThreads(), deserializedStats.getThreads());
                        assertEquals(stats.getActive(), deserializedStats.getActive());
                        assertEquals(stats.getLargest(), deserializedStats.getLargest());
                        assertEquals(stats.getCompleted(), deserializedStats.getCompleted());
                        assertEquals(stats.getQueue(), deserializedStats.getQueue());
                        assertEquals(stats.getRejected(), deserializedStats.getRejected());
                    }
                }
                if (nodeStats.getFs() == null) {
                    assertNull(deserializedNodeStats.getFs());
                } else {
                    assertEquals(nodeStats.getFs().getTimestamp(), deserializedNodeStats.getFs().getTimestamp());
                    assertEquals(nodeStats.getFs().getTotal().getAvailable(), deserializedNodeStats.getFs().getTotal().getAvailable());
                    assertEquals(nodeStats.getFs().getTotal().getTotal(), deserializedNodeStats.getFs().getTotal().getTotal());
                    assertEquals(nodeStats.getFs().getTotal().getFree(), deserializedNodeStats.getFs().getTotal().getFree());
                    assertEquals(nodeStats.getFs().getTotal().getMount(), deserializedNodeStats.getFs().getTotal().getMount());
                    assertEquals(nodeStats.getFs().getTotal().getPath(), deserializedNodeStats.getFs().getTotal().getPath());
                    assertEquals(nodeStats.getFs().getTotal().getSpins(), deserializedNodeStats.getFs().getTotal().getSpins());
                    assertEquals(nodeStats.getFs().getTotal().getType(), deserializedNodeStats.getFs().getTotal().getType());
                    assertEquals(nodeStats.getFs().getIoStats().getTotalOperations(),
                            deserializedNodeStats.getFs().getIoStats().getTotalOperations());
                    assertEquals(nodeStats.getFs().getIoStats().getTotalReadKilobytes(),
                            deserializedNodeStats.getFs().getIoStats().getTotalReadKilobytes());
                    assertEquals(nodeStats.getFs().getIoStats().getTotalReadOperations(),
                            deserializedNodeStats.getFs().getIoStats().getTotalReadOperations());
                    assertEquals(nodeStats.getFs().getIoStats().getTotalWriteKilobytes(),
                            deserializedNodeStats.getFs().getIoStats().getTotalWriteKilobytes());
                    assertEquals(nodeStats.getFs().getIoStats().getTotalWriteOperations(),
                            deserializedNodeStats.getFs().getIoStats().getTotalWriteOperations());
                    assertEquals(nodeStats.getFs().getIoStats().getDevicesStats().length,
                            deserializedNodeStats.getFs().getIoStats().getDevicesStats().length);
                    for (int i = 0; i < nodeStats.getFs().getIoStats().getDevicesStats().length; i++) {
                        FsInfo.DeviceStats deviceStats = nodeStats.getFs().getIoStats().getDevicesStats()[i];
                        FsInfo.DeviceStats deserializedDeviceStats = deserializedNodeStats.getFs().getIoStats().getDevicesStats()[i];
                        assertEquals(deviceStats.operations(), deserializedDeviceStats.operations());
                        assertEquals(deviceStats.readKilobytes(), deserializedDeviceStats.readKilobytes());
                        assertEquals(deviceStats.readOperations(), deserializedDeviceStats.readOperations());
                        assertEquals(deviceStats.writeKilobytes(), deserializedDeviceStats.writeKilobytes());
                        assertEquals(deviceStats.writeOperations(), deserializedDeviceStats.writeOperations());
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
                }
                if (nodeStats.getHttp() == null) {
                    assertNull(deserializedNodeStats.getHttp());
                } else {
                    assertEquals(nodeStats.getHttp().getServerOpen(), deserializedNodeStats.getHttp().getServerOpen());
                    assertEquals(nodeStats.getHttp().getTotalOpen(), deserializedNodeStats.getHttp().getTotalOpen());
                }
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
                if (nodeStats.getScriptStats() == null) {
                    assertNull(deserializedNodeStats.getScriptStats());
                } else {
                    assertEquals(nodeStats.getScriptStats().getCacheEvictions(),
                            deserializedNodeStats.getScriptStats().getCacheEvictions());
                    assertEquals(nodeStats.getScriptStats().getCompilations(), deserializedNodeStats.getScriptStats().getCompilations());
                }
                if (nodeStats.getDiscoveryStats() == null) {
                    assertNull(deserializedNodeStats.getDiscoveryStats());
                } else {
                    if (nodeStats.getDiscoveryStats().getQueueStats() == null) {
                        assertNull(deserializedNodeStats.getDiscoveryStats().getQueueStats());
                    } else {
                        assertEquals(nodeStats.getDiscoveryStats().getQueueStats().getCommitted(),
                                deserializedNodeStats.getDiscoveryStats().getQueueStats().getCommitted());
                        assertEquals(nodeStats.getDiscoveryStats().getQueueStats().getTotal(),
                                deserializedNodeStats.getDiscoveryStats().getQueueStats().getTotal());
                        assertEquals(nodeStats.getDiscoveryStats().getQueueStats().getPending(),
                                deserializedNodeStats.getDiscoveryStats().getQueueStats().getPending());
                    }
                }
                if (nodeStats.getIngestStats() == null) {
                    assertNull(deserializedNodeStats.getIngestStats());
                } else {
                    assertEquals(nodeStats.getIngestStats().getTotalStats().getIngestCount(),
                            deserializedNodeStats.getIngestStats().getTotalStats().getIngestCount());
                    assertEquals(nodeStats.getIngestStats().getTotalStats().getIngestCurrent(),
                            deserializedNodeStats.getIngestStats().getTotalStats().getIngestCurrent());
                    assertEquals(nodeStats.getIngestStats().getTotalStats().getIngestFailedCount(),
                            deserializedNodeStats.getIngestStats().getTotalStats().getIngestFailedCount());
                    assertEquals(nodeStats.getIngestStats().getTotalStats().getIngestTimeInMillis(),
                            deserializedNodeStats.getIngestStats().getTotalStats().getIngestTimeInMillis());
                    assertEquals(nodeStats.getIngestStats().getStatsPerPipeline().size(),
                            deserializedNodeStats.getIngestStats().getStatsPerPipeline().size());
                    for (Map.Entry<String, IngestStats.Stats> entry : nodeStats.getIngestStats().getStatsPerPipeline().entrySet()) {
                        IngestStats.Stats stats = entry.getValue();
                        IngestStats.Stats deserializedStats = deserializedNodeStats.getIngestStats()
                                .getStatsPerPipeline().get(entry.getKey());
                        assertEquals(stats.getIngestFailedCount(), deserializedStats.getIngestFailedCount());
                        assertEquals(stats.getIngestTimeInMillis(), deserializedStats.getIngestTimeInMillis());
                        assertEquals(stats.getIngestCurrent(), deserializedStats.getIngestCurrent());
                        assertEquals(stats.getIngestCount(), deserializedStats.getIngestCount());
                    }
                }
            }
        }
    }

    private static NodeStats createNodeStats() {
        DiscoveryNode node = new DiscoveryNode("test_node", LocalTransportAddress.buildUnique(),
                emptyMap(), emptySet(), VersionUtils.randomVersion(random()));
        OsStats osStats = null;
        if (frequently()) {
            double loadAverages[] = new double[3];
            for (int i = 0; i < 3; i++) {
                loadAverages[i] = randomBoolean() ? randomDouble() : -1;
            }
            osStats = new OsStats(System.currentTimeMillis(), new OsStats.Cpu(randomShort(), loadAverages),
                    new OsStats.Mem(randomLong(), randomLong()),
                    new OsStats.Swap(randomLong(), randomLong()));
        }
        ProcessStats processStats = frequently() ? new ProcessStats(randomPositiveLong(), randomPositiveLong(), randomPositiveLong(),
                new ProcessStats.Cpu(randomShort(), randomPositiveLong()),
                new ProcessStats.Mem(randomPositiveLong())) : null;
        JvmStats jvmStats = null;
        if (frequently()) {
            int numMemoryPools = randomIntBetween(0, 10);
            JvmStats.MemoryPool[] memoryPools = new JvmStats.MemoryPool[numMemoryPools];
            for (int i = 0; i < numMemoryPools; i++) {
                memoryPools[i] = new JvmStats.MemoryPool(randomAsciiOfLengthBetween(3, 10), randomPositiveLong(),
                        randomPositiveLong(), randomPositiveLong(), randomPositiveLong());
            }
            JvmStats.Threads threads = new JvmStats.Threads(randomIntBetween(1, 1000), randomIntBetween(1, 1000));
            int numGarbageCollectors = randomIntBetween(0, 10);
            JvmStats.GarbageCollector[] garbageCollectorsArray = new JvmStats.GarbageCollector[numGarbageCollectors];
            for (int i = 0; i < numGarbageCollectors; i++) {
                garbageCollectorsArray[i] = new JvmStats.GarbageCollector(randomAsciiOfLengthBetween(3, 10),
                        randomPositiveLong(), randomPositiveLong());
            }
            JvmStats.GarbageCollectors garbageCollectors = new JvmStats.GarbageCollectors(garbageCollectorsArray);
            int numBufferPools = randomIntBetween(0, 10);
            List<JvmStats.BufferPool> bufferPoolList = new ArrayList<>();
            for (int i = 0; i < numBufferPools; i++) {
                bufferPoolList.add(new JvmStats.BufferPool(randomAsciiOfLengthBetween(3, 10), randomPositiveLong(), randomPositiveLong(),
                        randomPositiveLong()));
            }
            JvmStats.Classes classes = new JvmStats.Classes(randomPositiveLong(), randomPositiveLong(), randomPositiveLong());
            jvmStats = frequently() ? new JvmStats(randomPositiveLong(), randomPositiveLong(), new JvmStats.Mem(randomPositiveLong(),
                    randomPositiveLong(), randomPositiveLong(), randomPositiveLong(), randomPositiveLong(), memoryPools), threads,
                    garbageCollectors, randomBoolean() ? null : bufferPoolList, classes) : null;
        }
        ThreadPoolStats threadPoolStats = null;
        if (frequently()) {
            int numThreadPoolStats = randomIntBetween(0, 10);
            List<ThreadPoolStats.Stats> threadPoolStatsList = new ArrayList<>();
            for (int i = 0; i < numThreadPoolStats; i++) {
                threadPoolStatsList.add(new ThreadPoolStats.Stats(randomAsciiOfLengthBetween(3, 10), randomIntBetween(1, 1000),
                        randomIntBetween(1, 1000), randomIntBetween(1, 1000), randomPositiveLong(),
                        randomIntBetween(1, 1000), randomIntBetween(1, 1000)));
            }
            threadPoolStats = new ThreadPoolStats(threadPoolStatsList);
        }
        FsInfo fsInfo = null;
        if (frequently()) {
            int numDeviceStats = randomIntBetween(0, 10);
            FsInfo.DeviceStats[] deviceStatsArray = new FsInfo.DeviceStats[numDeviceStats];
            for (int i = 0; i < numDeviceStats; i++) {
                FsInfo.DeviceStats previousDeviceStats = randomBoolean() ? null :
                        new FsInfo.DeviceStats(randomInt(), randomInt(), randomAsciiOfLengthBetween(3, 10),
                                randomPositiveLong(), randomPositiveLong(), randomPositiveLong(), randomPositiveLong(), null);
                deviceStatsArray[i] = new FsInfo.DeviceStats(randomInt(), randomInt(), randomAsciiOfLengthBetween(3, 10),
                        randomPositiveLong(), randomPositiveLong(), randomPositiveLong(), randomPositiveLong(), previousDeviceStats);
            }
            FsInfo.IoStats ioStats = new FsInfo.IoStats(deviceStatsArray);
            int numPaths = randomIntBetween(0, 10);
            FsInfo.Path[] paths = new FsInfo.Path[numPaths];
            for (int i = 0; i < numPaths; i++) {
                paths[i] = new FsInfo.Path(randomAsciiOfLengthBetween(3, 10), randomBoolean() ? randomAsciiOfLengthBetween(3, 10) : null,
                        randomPositiveLong(), randomPositiveLong(), randomPositiveLong());
            }
            fsInfo = new FsInfo(randomPositiveLong(), ioStats, paths);
        }
        TransportStats transportStats = frequently() ? new TransportStats(randomPositiveLong(), randomPositiveLong(),
                randomPositiveLong(), randomPositiveLong(), randomPositiveLong()) : null;
        HttpStats httpStats = frequently() ? new HttpStats(randomPositiveLong(), randomPositiveLong()) : null;
        AllCircuitBreakerStats allCircuitBreakerStats = null;
        if (frequently()) {
            int numCircuitBreakerStats = randomIntBetween(0, 10);
            CircuitBreakerStats[] circuitBreakerStatsArray = new CircuitBreakerStats[numCircuitBreakerStats];
            for (int i = 0; i < numCircuitBreakerStats; i++) {
                circuitBreakerStatsArray[i] = new CircuitBreakerStats(randomAsciiOfLengthBetween(3, 10), randomPositiveLong(),
                        randomPositiveLong(), randomDouble(), randomPositiveLong());
            }
            allCircuitBreakerStats = new AllCircuitBreakerStats(circuitBreakerStatsArray);
        }
        ScriptStats scriptStats = frequently() ? new ScriptStats(randomPositiveLong(), randomPositiveLong()) : null;
        DiscoveryStats discoveryStats = frequently() ? new DiscoveryStats(randomBoolean() ? new PendingClusterStateStats(randomInt(),
                randomInt(), randomInt()) : null) : null;
        IngestStats ingestStats = null;
        if (frequently()) {
            IngestStats.Stats totalStats = new IngestStats.Stats(randomPositiveLong(), randomPositiveLong(), randomPositiveLong(),
                    randomPositiveLong());

            int numStatsPerPipeline = randomIntBetween(0, 10);
            Map<String, IngestStats.Stats> statsPerPipeline = new HashMap<>();
            for (int i = 0; i < numStatsPerPipeline; i++) {
                statsPerPipeline.put(randomAsciiOfLengthBetween(3, 10), new IngestStats.Stats(randomPositiveLong(),
                        randomPositiveLong(), randomPositiveLong(), randomPositiveLong()));
            }
            ingestStats = new IngestStats(totalStats, statsPerPipeline);
        }
        //TODO NodeIndicesStats are not tested here, way too complicated to create, also they need to be migrated to Writeable yet
        return new NodeStats(node, randomPositiveLong(), null, osStats, processStats, jvmStats, threadPoolStats, fsInfo,
                transportStats, httpStats, allCircuitBreakerStats, scriptStats, discoveryStats, ingestStats);
    }
}
