/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm.history;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.slm.history.SnapshotLifecycleTemplateRegistry.INDEX_TEMPLATE_VERSION;

/**
 * Records Snapshot Lifecycle Management actions as represented by {@link SnapshotHistoryItem} into an index
 * for the purposes of querying and alerting.
 */
public class SnapshotHistoryStore {
    private static final Logger logger = LogManager.getLogger(SnapshotHistoryStore.class);
    private static final DateFormatter indexTimeFormat = DateFormatter.forPattern("yyyy.MM");

    public static final String SLM_HISTORY_INDEX_PREFIX = ".slm-history-" + INDEX_TEMPLATE_VERSION + "-";

    private final Client client;
    private final ZoneId timeZone;
    private final boolean slmHistoryEnabled;

    public SnapshotHistoryStore(Settings nodeSettings, Client client, ZoneId timeZone) {
        this.client = client;
        this.timeZone = timeZone;
        slmHistoryEnabled = SLM_HISTORY_INDEX_ENABLED_SETTING.get(nodeSettings);
    }

    /**
     * Attempts to asynchronously index a snapshot lifecycle management history entry
     *
     * @param item The entry to index
     */
    public void putAsync(SnapshotHistoryItem item) {
        if (slmHistoryEnabled == false) {
            logger.trace("not recording snapshot history item because [{}] is [false]: [{}]",
                SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), item);
            return;
        }
        final ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(item.getTimestamp()), timeZone);
        final String index = getHistoryIndexNameForTime(dateTime);
        logger.trace("about to index snapshot history item in index [{}]: [{}]", index, item);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            item.toXContent(builder, ToXContent.EMPTY_PARAMS);
            IndexRequest request = new IndexRequest(index)
                .source(builder);
            client.index(request, ActionListener.wrap(indexResponse -> {
                logger.debug("successfully indexed snapshot history item with id [{}] in index [{}]: [{}]",
                    indexResponse.getId(), index, item);
            }, exception -> {
                logger.error(new ParameterizedMessage("failed to index snapshot history item in index [{}]: [{}]",
                    index, item), exception);
            }));
        } catch (IOException exception) {
            logger.error(new ParameterizedMessage("failed to index snapshot history item in index [{}]: [{}]",
                index, item), exception);
        }
    }


    static String getHistoryIndexNameForTime(ZonedDateTime time) {
        return SLM_HISTORY_INDEX_PREFIX + indexTimeFormat.format(time);
    }
}
