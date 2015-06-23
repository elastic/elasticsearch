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
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.support.TemplateUtils;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 */
public class HistoryStore extends AbstractComponent implements NodeSettingsService.Listener {

    public static final String INDEX_PREFIX = ".watch_history-";
    public static final String DOC_TYPE = "watch_record";
    public static final String INDEX_TEMPLATE_NAME = "watch_history";

    static final DateTimeFormatter indexTimeFormat = DateTimeFormat.forPattern("YYYY.MM.dd");
    private static final ImmutableSet<String> forbiddenIndexSettings = ImmutableSet.of("index.mapper.dynamic");

    private final ClientProxy client;
    private final TemplateUtils templateUtils;
    private final ThreadPool threadPool;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock putUpdateLock = readWriteLock.readLock();
    private final Lock stopLock = readWriteLock.writeLock();
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile Settings customIndexSettings = Settings.EMPTY;

    @Inject
    public HistoryStore(Settings settings, ClientProxy client, TemplateUtils templateUtils, NodeSettingsService nodeSettingsService,
                        @ClusterDynamicSettings DynamicSettings dynamicSettings, ThreadPool threadPool) {
        super(settings);
        this.client = client;
        this.templateUtils = templateUtils;
        this.threadPool = threadPool;

        updateHistorySettings(settings, false);
        nodeSettingsService.addListener(this);
        dynamicSettings.addDynamicSetting("watcher.history.index.*");
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        updateHistorySettings(settings, true);
    }

    private void updateHistorySettings(Settings settings, boolean updateIndexTemplate) {
        Settings newSettings = Settings.builder()
                .put(settings.getAsSettings("watcher.history.index"))
                .build();
        if (newSettings.names().isEmpty()) {
            return;
        }

        boolean changed = false;
        Settings.Builder builder = Settings.builder().put(customIndexSettings);

        for (Map.Entry<String, String> entry : newSettings.getAsMap().entrySet()) {
            String name = "index." + entry.getKey();
            if (forbiddenIndexSettings.contains(name)) {
                logger.warn("overriding the default [{}} setting is forbidden. ignoring...", name);
                continue;
            }

            String newValue = entry.getValue();
            String currentValue = customIndexSettings.get(name);
            if (!newValue.equals(currentValue)) {
                changed = true;
                builder.put(name, newValue);
                logger.info("changing setting [{}] from [{}] to [{}]", name, currentValue, newValue);
            }
        }

        if (changed) {
            customIndexSettings = builder.build();
            if (updateIndexTemplate) {
                // Need to fork to prevent dead lock. (We're on the cluster service update task, but the put index template
                // needs to update the cluster state too, and because the update takes is a single threaded operation,
                // we would then be stuck)
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                    @Override
                    public void run() {
                        templateUtils.putTemplate(INDEX_TEMPLATE_NAME, customIndexSettings);
                    }
                });
            }
        }
    }


    public void start() {
        if (started.compareAndSet(false, true)) {
            try {
                templateUtils.putTemplate(INDEX_TEMPLATE_NAME, customIndexSettings);
            } catch (Exception e) {
                started.set(false);
                throw e;
            }
        }
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

    public void put(WatchRecord watchRecord) throws HistoryException {
        if (!started.get()) {
            throw new HistoryException("unable to persist watch record history store is not ready");
        }
        String index = getHistoryIndexNameForTime(watchRecord.triggerEvent().triggeredTime());
        putUpdateLock.lock();
        try {
            IndexRequest request = new IndexRequest(index, DOC_TYPE, watchRecord.id().value())
                    .source(XContentFactory.jsonBuilder().value(watchRecord))
                    .opType(IndexRequest.OpType.CREATE);
            client.index(request, (TimeValue) null);
        } catch (IOException e) {
            throw new HistoryException("failed to persist watch record [" + watchRecord + "]", e);
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
