/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.history;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.watcher.support.Exceptions.ioException;

public class HistoryStore extends AbstractComponent implements Closeable {

    private static final Setting<Integer> SETTING_BULK_ACTIONS =
        Setting.intSetting("xpack.watcher.history_store.bulk.actions", 1, 1, 10000, NodeScope);
    private static final Setting<Integer> SETTING_BULK_CONCURRENT_REQUESTS =
        Setting.intSetting("xpack.watcher.history_store.bulk.concurrent_requests", 0, 0, 20, NodeScope);
    private static final Setting<TimeValue> SETTING_BULK_FLUSH_INTERVAL =
        Setting.timeSetting("xpack.watcher.history_store.bulk.flush_interval", TimeValue.timeValueSeconds(1), NodeScope);
    private static final Setting<ByteSizeValue> SETTING_BULK_SIZE =
        Setting.byteSizeSetting("xpack.watcher.history_store.bulk.size", new ByteSizeValue(1, ByteSizeUnit.MB),
            new ByteSizeValue(1, ByteSizeUnit.MB), new ByteSizeValue(10, ByteSizeUnit.MB), NodeScope);

    public static final String DOC_TYPE = "doc";

    private final BulkProcessor bulkProcessor;

    public HistoryStore(Settings settings, Client client) {
        super(settings);
        this.bulkProcessor = BulkProcessor.builder(ClientHelper.clientWithOrigin(client, WATCHER_ORIGIN), new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    logger.error("storing watch history entry failed [{}]", response.buildFailureMessage());
                }
                List<String> overwrittenIds = Arrays.stream(response.getItems())
                    .filter(r -> r.getVersion() > 1)
                    .map(BulkItemResponse::getId)
                    .collect(Collectors.toList());
                if (overwrittenIds.isEmpty() == false) {
                    logger.info("overwrote watch history entries {}, possible second execution of a triggered watch", overwrittenIds);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("error storing watch history entries", failure);
            }
        })
            .setFlushInterval(SETTING_BULK_FLUSH_INTERVAL.get(settings))
            .setBulkActions(SETTING_BULK_ACTIONS.get(settings))
            .setBulkSize(SETTING_BULK_SIZE.get(settings))
            .setConcurrentRequests(SETTING_BULK_CONCURRENT_REQUESTS.get(settings))
            .build();

    }

    @Override
    public void close() throws IOException {
        bulkProcessor.flush();
        try {
            if (bulkProcessor.awaitClose(10, TimeUnit.SECONDS) == false) {
                logger.warn("triggered watch store failed to delete triggered watches after waiting for 10s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stores the specified watchRecord.
     * If the specified watchRecord already was stored this call will fail with a version conflict.
     */
    public void put(WatchRecord watchRecord) throws Exception {
        String index = HistoryStoreField.getHistoryIndexNameForTime(watchRecord.triggerEvent().triggeredTime());
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            watchRecord.toXContent(builder, WatcherParams.HIDE_SECRETS);

            IndexRequest request = new IndexRequest(index, DOC_TYPE, watchRecord.id().value()).source(builder);
            request.opType(IndexRequest.OpType.CREATE);
            bulkProcessor.add(request);
        } catch (IOException ioe) {
            throw ioException("failed to persist watch record [{}]", ioe, watchRecord);
        }
    }

    /**
     * Stores the specified watchRecord.
     * Any existing watchRecord will be overwritten.
     */
    public void forcePut(WatchRecord watchRecord) {
        String index = HistoryStoreField.getHistoryIndexNameForTime(watchRecord.triggerEvent().triggeredTime());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                watchRecord.toXContent(builder, WatcherParams.HIDE_SECRETS);

                IndexRequest request = new IndexRequest(index, DOC_TYPE, watchRecord.id().value()).source(builder);
                bulkProcessor.add(request);
        } catch (IOException ioe) {
            final WatchRecord wr = watchRecord;
            logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to persist watch record [{}]", wr), ioe);
        }
    }

    /**
     * Check if everything is set up for the history store to operate fully. Checks for the
     * current watcher history index and if it is open.
     *
     * @param state The current cluster state
     * @return true, if history store is ready to be started
     */
    public static boolean validate(ClusterState state) {
        String currentIndex = HistoryStoreField.getHistoryIndexNameForTime(DateTime.now(DateTimeZone.UTC));
        IndexMetaData indexMetaData = WatchStoreUtils.getConcreteIndex(currentIndex, state.metaData());
        return indexMetaData == null || (indexMetaData.getState() == IndexMetaData.State.OPEN &&
            state.routingTable().index(indexMetaData.getIndex()).allPrimaryShardsActive());
    }

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(SETTING_BULK_ACTIONS, SETTING_BULK_CONCURRENT_REQUESTS, SETTING_BULK_FLUSH_INTERVAL, SETTING_BULK_SIZE);
    }
}
