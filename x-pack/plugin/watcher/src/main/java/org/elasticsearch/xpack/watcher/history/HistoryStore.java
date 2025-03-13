/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.history;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.io.IOException;

import static org.elasticsearch.common.settings.Setting.Property.NodeScope;
import static org.elasticsearch.xpack.core.watcher.support.Exceptions.ioException;

public class HistoryStore {

    private static final Logger logger = LogManager.getLogger(HistoryStore.class);
    public static final Setting<ByteSizeValue> MAX_HISTORY_SIZE_SETTING = Setting.byteSizeSetting(
        "xpack.watcher.max.history.record.size",
        ByteSizeValue.ofMb(10),
        NodeScope
    );

    private final BulkProcessor2 bulkProcessor;
    private final ByteSizeValue maxHistoryRecordSize;

    public HistoryStore(BulkProcessor2 bulkProcessor, Settings settings) {
        this.bulkProcessor = bulkProcessor;
        maxHistoryRecordSize = MAX_HISTORY_SIZE_SETTING.get(settings);
    }

    /**
     * Stores the specified watchRecord.
     * If the specified watchRecord already was stored this call will fail with a version conflict.
     */
    public void put(WatchRecord watchRecord) throws Exception {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            watchRecord.toXContent(builder, WatcherParams.HIDE_SECRETS);
            IndexRequest request = new IndexRequest(HistoryStoreField.DATA_STREAM).id(watchRecord.id().value()).source(builder);
            request.opType(IndexRequest.OpType.CREATE);
            if (request.source().length() > maxHistoryRecordSize.getBytes()) {
                WatchRecord redactedWatchRecord = watchRecord.dropLargeFields();
                try (XContentBuilder redactedBuilder = XContentFactory.jsonBuilder()) {
                    redactedWatchRecord.toXContent(redactedBuilder, WatcherParams.HIDE_SECRETS);
                    request.source(redactedBuilder);
                }
            }
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
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            watchRecord.toXContent(builder, WatcherParams.HIDE_SECRETS);

            IndexRequest request = new IndexRequest(HistoryStoreField.DATA_STREAM).id(watchRecord.id().value()).source(builder);
            request.opType(DocWriteRequest.OpType.CREATE);
            bulkProcessor.add(request);
        } catch (IOException ioe) {
            final WatchRecord wr = watchRecord;
            logger.error(() -> "failed to persist watch record [" + wr + "]", ioe);
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
        IndexMetadata indexMetadata = WatchStoreUtils.getConcreteIndex(HistoryStoreField.DATA_STREAM, state.metadata());
        return indexMetadata == null
            || (indexMetadata.getState() == IndexMetadata.State.OPEN
                && state.routingTable().index(indexMetadata.getIndex()).allPrimaryShardsActive());
    }
}
