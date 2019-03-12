/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.history;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.core.watcher.support.Exceptions.ioException;

public class HistoryStore {

    private static final Logger logger = LogManager.getLogger(HistoryStore.class);

    private final BulkProcessor bulkProcessor;

    public HistoryStore(BulkProcessor bulkProcessor) {
        this.bulkProcessor = bulkProcessor;
    }

    /**
     * Stores the specified watchRecord.
     * If the specified watchRecord already was stored this call will fail with a version conflict.
     */
    public void put(WatchRecord watchRecord) throws Exception {
        String index = HistoryStoreField.getHistoryIndexNameForTime(watchRecord.triggerEvent().triggeredTime());
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            watchRecord.toXContent(builder, WatcherParams.HIDE_SECRETS);

            IndexRequest request = new IndexRequest(index).id(watchRecord.id().value()).source(builder);
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

                IndexRequest request = new IndexRequest(index).id(watchRecord.id().value()).source(builder);
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
        String currentIndex = HistoryStoreField.getHistoryIndexNameForTime(ZonedDateTime.now(ZoneOffset.UTC));
        IndexMetaData indexMetaData = WatchStoreUtils.getConcreteIndex(currentIndex, state.metaData());
        return indexMetaData == null || (indexMetaData.getState() == IndexMetaData.State.OPEN &&
            state.routingTable().index(indexMetaData.getIndex()).allPrimaryShardsActive());
    }
}
