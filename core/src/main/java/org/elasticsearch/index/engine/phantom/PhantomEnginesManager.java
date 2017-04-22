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

package org.elasticsearch.index.engine.phantom;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Phantom engines manager holds list of all loaded phantom engines and tracks used heap by them.
 * When this usage reaches specified limit, it searches the eldest loaded engine between less frequently
 * used. This action is performed in {@link #add(PhantomEngine)} in blocking fashion.
 *
 * @author ikuznetsov
 */
public class PhantomEnginesManager implements ClusterStateListener {

    public static final String PHANTOM_INDICES_MAX_HEAP_SIZE = "index.phantom.max_heap_size";

    private final Map<ShardId, PhantomEngine> loadedEngines = new ConcurrentHashMap<>(100);
    private final ESLogger logger = Loggers.getLogger(PhantomEnginesManager.class);
    private final AtomicLong currentSize = new AtomicLong(0);
    private final ReentrantLock addLock = new ReentrantLock();
    private final ReentrantLock removeLock = new ReentrantLock();
    private final ThreadPool threadPool;

    // counters
    final AtomicLong addTime = new AtomicLong(0);
    final AtomicLong addCount = new AtomicLong(0);
    final AtomicLong removeTime = new AtomicLong(0);
    final AtomicLong removeCount = new AtomicLong(0);

    // dynamically updateable
    private final AtomicLong maxHeapSize = new AtomicLong(0);

    @Inject
    public PhantomEnginesManager(Settings settings, ClusterService clusterService, ThreadPool threadPool) {
        this.threadPool = threadPool;
        updateSettings(settings);
        clusterService.add(this);

        logger.debug("using heap size limit for phantom indices [{}]", maxHeapSize());
    }

    void add(PhantomEngine engine) {
        // do nothing if engine has already been loaded
        if (loadedEngines.containsKey(engine.shardId())) {
            return;
        }

        // include wait
        long startTime = System.nanoTime();

        // atomically add engine to list and
        // change used reserved heap size
        addLock.lock();

        // check available reserved heap for phantom engines
        if (isNotEnoughPhantomHeap(engine)) {
            unloadLeastUsedToAddEngine(engine);
        }

        // free reserved heap is present, do engine load
        engine.load();

        // remember that we loaded it
        loadedEngines.put(engine.shardId(), engine);
        currentSize.addAndGet(engine.usedHeapSizeInBytes());

        addCount.incrementAndGet();
        addTime.addAndGet((long) ((System.nanoTime() - startTime) / 1e6));
        logger.debug("added engine for {} to loaded engines list", engine.shardId());

        addLock.unlock();
    }

    void remove(PhantomEngine engine) {
        // include wait
        long startTime = System.nanoTime();

        removeLock.lock(); // atomically remove engine from list and change counter

        PhantomEngine removed = loadedEngines.remove(engine.shardId());

        if (removed != null) {
            // engine is unloaded
            currentSize.addAndGet(-engine.usedHeapSizeInBytes());
            logger.debug("removed engine for {} from loaded engines list, current phantom engine's size: [{}]", engine.shardId(), currentSize);
        }

        removeCount.incrementAndGet();
        removeTime.addAndGet((long) ((System.nanoTime() - startTime) / 1e6));
        removeLock.unlock();
    }

    /**
     * Checks whether adding phantom engine can be loaded.
     * It can be loaded when delta of currently used reserved heap by loaded phantom engines
     * and max heap size for phantom engines can cover required heap size for adding engine.
     * When adding engine is null, works as {@link #isNotEnoughPhantomHeap()}.
     *
     * @param addingEngine engine that need to be loaded.
     * @return true if there is not enough free reserved heap to add given engine, false otherwise.
     */
    private boolean isNotEnoughPhantomHeap(PhantomEngine addingEngine) {
        long engineSize = addingEngine != null ? addingEngine.usedHeapSizeInBytes() : 0;
        return currentSize.get() +  engineSize >= maxHeapSize.get();
    }

    /**
     * Checks that current heap size used by loaded phantom engines exceed its limit.
     *
     * @return true if loaded engines exceed reserved heap size, false otherwise.
     */
    private boolean isNotEnoughPhantomHeap() {
        return isNotEnoughPhantomHeap(null);
    }

    /**
     * Returns free heap size available for phantom engines
     * - currently used heap size by phantom engines minus
     * it's size limit.
     *
     * @return free heap size available for phantom engines.
     */
    private ByteSizeValue freeHeapSize() {
        return ByteSizeValue.of(maxHeapSize.get() - currentSize.get());
    }

    /**
     * Returns remaining heap size needed to load given engine
     * - engine heap size minus free heap size for phantom engines.
     *
     * @param addingEngine engine that is required to load.
     * @return remaining heap size needed to load engine.
     */
    private ByteSizeValue remainingFreeHeapSizeFor(PhantomEngine addingEngine) {
        return ByteSizeValue.of(addingEngine.usedHeapSizeInBytes() - freeHeapSize().bytes());
    }

    private void unloadLeastUsed() {
        unloadLeastUsed(null);
    }

    private void unloadLeastUsedToAddEngine(PhantomEngine addingEngine) {
        if (addingEngine.usedHeapSizeInBytes() > maxHeapSize.get()) {
            String msg = String.format(Locale.ROOT, "Shard %s could not be loaded: engine with size [%s]" +
                    " exceeds heap limit [%s] for phantom engines",
                addingEngine.shardId(), addingEngine.usedHeapSize(), maxHeapSize());
            throw new RuntimeException(msg);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("adding of {} requires additional {} of free heap (adding engine heap [{}], free heap [{}])",
                addingEngine.shardId(), remainingFreeHeapSizeFor(addingEngine), addingEngine.usedHeapSize(), freeHeapSize());
        }

        unloadLeastUsed(addingEngine);
    }

    private void unloadLeastUsed(PhantomEngine addingEngine) {
        PhantomEngine removing = null;

        // find all engines with min hits ...

        Map<Long, List<PhantomEngine>> enginesByHits = new HashMap<>();
        long minHits = -1;
        for (PhantomEngine phantomEngine : loadedEngines.values()) {
            long cachedHits = phantomEngine.hits(); // it can change during search on other shards
            if (removing == null) {
                minHits = cachedHits;
                removing = phantomEngine;
                putToMap(enginesByHits, phantomEngine, cachedHits);
                logger.debug("initial engine to unload: {} with hits [{}]", removing.shardId(), removing.hits());
                continue;
            }

            if (cachedHits < minHits) {
                minHits = cachedHits;
                putToMap(enginesByHits, phantomEngine, cachedHits);
                removing = phantomEngine;
                logger.debug("found engine with less hits: {} with [{}]", removing.shardId(), removing.hits());
            } else if (cachedHits == minHits) {
                putToMap(enginesByHits, phantomEngine, cachedHits);
                logger.debug("found another engine with the same hits [{}]: {}", cachedHits, phantomEngine.shardId());
            }
        }

        // ... and remove from the unused to used in searches / eldest to recently loaded engine
        // until it become enough to load adding engine

        List<PhantomEngine> phantomEnginesWithMinHits = enginesByHits.get(minHits);
        logger.debug("collected [{}] engines with hits [min {}/real {}] for unloading", phantomEnginesWithMinHits.size(), minHits, removing.hits());

        // unused -> searching first, eldest -> recently loaded last
        Collections.sort(phantomEnginesWithMinHits, new Comparator<PhantomEngine>() {
            @Override
            public int compare(PhantomEngine e1, PhantomEngine e2) {
                int ufo = Boolean.compare(e1.hasActiveSearches(), e2.hasActiveSearches());
                if (ufo == 0) {
                    return (int) (e1.loadTimestamp() - e2.loadTimestamp());
                }
                return ufo;
            }
        });

        Iterator<PhantomEngine> iterator = phantomEnginesWithMinHits.iterator();
        do {
            PhantomEngine phantomEngine = iterator.next();
            phantomEngine.unload(); // unload and close
            remove(phantomEngine);
            if (logger.isDebugEnabled()) {
                logger.debug("unloaded engine {} with timestamp: [{}], searching [{}]", phantomEngine.shardId(), phantomEngine.loadTimestamp(), phantomEngine.hasActiveSearches());
                if (addingEngine != null) {
                    logger.debug("current phantom free heap [{}], adding engine heap [{}], continue? [{}]", freeHeapSize(), addingEngine.usedHeapSize(), isNotEnoughPhantomHeap(addingEngine));
                } else {
                    logger.debug("current phantom free heap [{}], continue? [{}]", freeHeapSize(), isNotEnoughPhantomHeap(null));
                }
            }
        } while (iterator.hasNext() && isNotEnoughPhantomHeap(addingEngine));

        // all engines with current min hits were fully unloaded here,
        // it can be big shard or unloaded shards were too small for loading one

        if (isNotEnoughPhantomHeap(addingEngine)) {
            // again go through engines and find ones with min hits
            logger.debug("unloaded all engines with hits [{}], going to unload engines again");
            unloadLeastUsed(addingEngine);
        }
    }

    private static void putToMap(Map<Long, List<PhantomEngine>> enginesByHits, PhantomEngine engine, long cachedHits) {
        List<PhantomEngine> engines = enginesByHits.get(cachedHits);
        if (engines == null) {
            engines = new ArrayList<>();
            enginesByHits.put(cachedHits, engines);
        }
        engines.add(engine);
        enginesByHits.put(cachedHits, engines);
    }

    public PhantomEnginesManagerStats stats() {
        return new PhantomEnginesManagerStats(this);
    }

    long usedHeapSizeInBytes() {
        return currentSize.get();
    }

    long maxHeapSizeInBytes() {
        return maxHeapSize.get();
    }

    ByteSizeValue maxHeapSize() {
        return ByteSizeValue.of(maxHeapSize.get());
    }

    int loadedEnginesCount() {
        return loadedEngines.size();
    }

    private boolean updateSettings(Settings settings) {
        JvmInfo jvmInfo = JvmInfo.jvmInfo();
        long heapSize = jvmInfo.getMem().getHeapMax().bytes();

        String raw = settings.get(PHANTOM_INDICES_MAX_HEAP_SIZE);
        if (raw != null) {
            // deny legal "1" value
            if (!raw.endsWith("%") && Character.isDigit(raw.charAt(raw.length() - 1))) {
                throw new IllegalArgumentException("Expected percent value or size in bytes");
            }
        }

        ByteSizeValue maxHeapSize;
        try {
            double ratio = settings.getAsRatio(PHANTOM_INDICES_MAX_HEAP_SIZE, "20%").getAsRatio();
            maxHeapSize = ByteSizeValue.parseBytesSizeValue(Math.round(ratio * heapSize) + "b", "");
        } catch (ElasticsearchParseException e) {
            maxHeapSize = settings.getAsBytesSize(PHANTOM_INDICES_MAX_HEAP_SIZE, null);
        }

        if (maxHeapSize == null) {
            throw new ElasticsearchParseException("Couldn't parse value [" + raw + "]; expected percent value or size in bytes");
        }

        long maxHeapSizeInBytes = maxHeapSize.bytes();

        if (this.maxHeapSize.get() == maxHeapSizeInBytes) {
            return false;
        }

        this.maxHeapSize.set(maxHeapSizeInBytes);

        threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
            @Override
            public void run() {
                while (isNotEnoughPhantomHeap()) {
                    unloadLeastUsed();
                }
            }
        });

        return true;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }

        addLock.lock();
        try {
            if (updateSettings(event.state().metaData().settings())) {
                logger.info("updated heap size limit for phantom indices to [{}]", maxHeapSize());
            }
        } finally {
            addLock.unlock();
        }
    }
}
