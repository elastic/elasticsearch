/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.watcher.support.Exceptions.ioException;

/**
 */
public class HistoryStore extends AbstractComponent {

    public static final String INDEX_PREFIX = ".watch_history-";
    public static final String DOC_TYPE = "watch_record";

    static final DateTimeFormatter indexTimeFormat = DateTimeFormat.forPattern("YYYY.MM.dd");
    private static final ImmutableSet<String> forbiddenIndexSettings = ImmutableSet.of("index.mapper.dynamic");

    private final ClientProxy client;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock putUpdateLock = readWriteLock.readLock();
    private final Lock stopLock = readWriteLock.writeLock();
    private final AtomicBoolean started = new AtomicBoolean(false);

    @Inject
    public HistoryStore(Settings settings, ClientProxy client) {
        super(settings);
        this.client = client;
    }

    public void start() {
        started.set(true);
    }

    public boolean validate(ClusterState state) {
        String[] indices = state.metaData().concreteIndices(IndicesOptions.lenientExpandOpen(), INDEX_PREFIX + "*");
        if (indices.length == 0) {
            logger.debug("no history indices exist, so we can load");
            return true;
        }

        for (String index : indices) {
            IndexMetaData indexMetaData = state.getMetaData().index(index);
            if (indexMetaData != null) {
                if (!state.routingTable().index(index).allPrimaryShardsActive()) {
                    logger.debug("not all primary shards of the [{}] index are started, so we cannot load watcher records", index);
                    return false;
                }
            }
        }

        return true;
    }

    public void stop() {
        stopLock.lock(); //This will block while put or update actions are underway
        try {
            started.set(false);
        } finally {
            stopLock.unlock();
        }

    }

    public void put(WatchRecord watchRecord) throws Exception {
        if (!started.get()) {
            throw new IllegalStateException("unable to persist watch record history store is not ready");
        }
        String index = getHistoryIndexNameForTime(watchRecord.triggerEvent().triggeredTime());
        putUpdateLock.lock();
        try {
            IndexRequest request = new IndexRequest(index, DOC_TYPE, watchRecord.id().value())
                    .source(XContentFactory.jsonBuilder().value(watchRecord))
                    .opType(IndexRequest.OpType.CREATE);
            client.index(request, (TimeValue) null);
        } catch (IOException ioe) {
            throw ioException("failed to persist watch record [{}]", ioe, watchRecord);
        } finally {
            putUpdateLock.unlock();
        }
    }

    /**
     * Calculates the correct history index name for a given time
     */
    public static String getHistoryIndexNameForTime(DateTime time) {
        return INDEX_PREFIX + indexTimeFormat.print(time);
    }

}
