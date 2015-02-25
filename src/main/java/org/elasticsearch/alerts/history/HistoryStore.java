/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.alerts.support.TemplateUtils;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 */
public class HistoryStore extends AbstractComponent {

    public static final String ALERT_HISTORY_INDEX_PREFIX = ".alert_history_";
    public static final String ALERT_HISTORY_TYPE = "fired_alert";

    static final DateTimeFormatter alertHistoryIndexTimeFormat = DateTimeFormat.forPattern("YYYY-MM-dd");

    private final ClientProxy client;
    private final TemplateUtils templateUtils;
    private final int scrollSize;
    private final TimeValue scrollTimeout;
    private final FiredAlert.Parser alertRecordParser;

    @Inject
    public HistoryStore(Settings settings, ClientProxy client, TemplateUtils templateUtils, FiredAlert.Parser alertRecordParser) {
        super(settings);
        this.client = client;
        this.templateUtils = templateUtils;
        this.alertRecordParser = alertRecordParser;
        this.scrollTimeout = settings.getAsTime("alerts.scroll.timeout", TimeValue.timeValueSeconds(30));
        this.scrollSize = settings.getAsInt("alerts.scroll.size", 100);
    }

    public void put(FiredAlert firedAlert) throws HistoryException {
        String alertHistoryIndex = getAlertHistoryIndexNameForTime(firedAlert.scheduledTime());
        try {
            IndexResponse response = client.prepareIndex(alertHistoryIndex, ALERT_HISTORY_TYPE, firedAlert.id())
                    .setSource(XContentFactory.jsonBuilder().value(firedAlert))
                    .setOpType(IndexRequest.OpType.CREATE)
                    .get();
            firedAlert.version(response.getVersion());
        } catch (IOException e) {
            throw new HistoryException("persisting new fired alert [" + firedAlert + "] failed", e);
        }
    }

    public void update(FiredAlert firedAlert) throws HistoryException {
        logger.debug("updating fired alert [{}]", firedAlert);
        try {
            BytesReference bytes = XContentFactory.jsonBuilder().value(firedAlert).bytes();
            IndexResponse response = client.prepareIndex(getAlertHistoryIndexNameForTime(firedAlert.scheduledTime()), ALERT_HISTORY_TYPE, firedAlert.id())
                    .setSource(bytes)
                    .setVersion(firedAlert.version())
                    .get();
            firedAlert.version(response.getVersion());
            logger.debug("updated fired alert [{}]", firedAlert);
        } catch (IOException e) {
            throw new HistoryException("persisting fired alert [" + firedAlert + "] failed", e);
        }
    }

    public LoadResult loadFiredAlerts(ClusterState state, FiredAlert.State firedAlertState) {
        String[] indices = state.metaData().concreteIndices(IndicesOptions.lenientExpandOpen(), ALERT_HISTORY_INDEX_PREFIX + "*");
        if (indices.length == 0) {
            logger.debug("No .alert_history indices found, skip loading of alert actions");
            templateUtils.ensureIndexTemplateIsLoaded(state, "alerthistory");
            return new LoadResult(true);
        }
        int numPrimaryShards = 0;
        for (String index : indices) {
            IndexMetaData indexMetaData = state.getMetaData().index(index);
            if (indexMetaData != null) {
                if (!state.routingTable().index(index).allPrimaryShardsActive()) {
                    logger.debug("Not all primary shards of the [{}] index are started. Schedule to retry alert action loading..", index);
                    return new LoadResult(false);
                } else {
                    numPrimaryShards += indexMetaData.numberOfShards();
                }
            }
        }

        RefreshResponse refreshResponse = client.admin().indices().refresh(new RefreshRequest(ALERT_HISTORY_INDEX_PREFIX + "*")).actionGet();
        if (refreshResponse.getSuccessfulShards() < numPrimaryShards) {
            return new LoadResult(false);
        }

        SearchResponse response = client.prepareSearch(ALERT_HISTORY_INDEX_PREFIX + "*")
                .setQuery(QueryBuilders.termQuery(FiredAlert.Parser.STATE_FIELD.getPreferredName(), firedAlertState.id()))
                .setSearchType(SearchType.SCAN)
                .setScroll(scrollTimeout)
                .setSize(scrollSize)
                .setTypes(ALERT_HISTORY_TYPE)
                .setPreference("_primary")
                .setVersion(true)
                .get();
        List<FiredAlert> alerts = new ArrayList<>();
        try {
            if (response.getTotalShards() != response.getSuccessfulShards()) {
                return new LoadResult(false);
            }

            if (response.getHits().getTotalHits() > 0) {
                response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeout).get();
                while (response.getHits().hits().length != 0) {
                    for (SearchHit sh : response.getHits()) {
                        String historyId = sh.getId();
                        FiredAlert historyEntry = alertRecordParser.parse(sh.getSourceRef(), historyId, sh.version());
                        assert historyEntry.state() == FiredAlert.State.AWAITS_EXECUTION;
                        logger.debug("loaded fired alert from index [{}/{}/{}]", sh.index(), sh.type(), sh.id());
                        alerts.add(historyEntry);
                    }
                    response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeout).get();
                }
            }
        } finally {
            client.prepareClearScroll().addScrollId(response.getScrollId()).get();
        }
        templateUtils.ensureIndexTemplateIsLoaded(state, "alerthistory");
        return new LoadResult(true, alerts);
    }

    /**
     * Calculates the correct alert history index name for a given time using alertHistoryIndexTimeFormat
     */
    public static String getAlertHistoryIndexNameForTime(DateTime time) {
        return ALERT_HISTORY_INDEX_PREFIX + alertHistoryIndexTimeFormat.print(time);
    }

    public class LoadResult implements Iterable<FiredAlert> {

        private final boolean succeeded;
        private final List<FiredAlert> alerts;

        public LoadResult(boolean succeeded, List<FiredAlert> alerts) {
            this.succeeded = succeeded;
            this.alerts = alerts;
        }

        public LoadResult(boolean succeeded) {
            this.succeeded = succeeded;
            this.alerts = Collections.emptyList();
        }

        @Override
        public Iterator<FiredAlert> iterator() {
            return alerts.iterator();
        }

        public boolean succeeded() {
            return succeeded;
        }
    }
}
