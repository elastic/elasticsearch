/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.support.TemplateUtils;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class AlertsStore extends AbstractComponent {

    public static final String ALERT_INDEX = ".alerts";
    public static final String ALERT_INDEX_TEMPLATE = "alerts";
    public static final String ALERT_TYPE = "alert";

    private final ClientProxy client;
    private final TemplateUtils templateUtils;
    private final Alert.Parser alertParser;

    private final ConcurrentMap<String, Alert> alertMap;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final int scrollSize;
    private final TimeValue scrollTimeout;

    @Inject
    public AlertsStore(Settings settings, ClientProxy client, TemplateUtils templateUtils, Alert.Parser alertParser) {
        super(settings);
        this.client = client;
        this.templateUtils = templateUtils;
        this.alertParser = alertParser;
        this.alertMap = ConcurrentCollections.newConcurrentMap();

        this.scrollTimeout = componentSettings.getAsTime("scroll.timeout", TimeValue.timeValueSeconds(30));
        this.scrollSize = componentSettings.getAsInt("scroll.size", 100);
    }

    public boolean start(ClusterState state) {
        if (started.get()) {
            return true;
        }

        IndexMetaData alertIndexMetaData = state.getMetaData().index(ALERT_INDEX);
        if (alertIndexMetaData == null) {
            logger.trace("alerts index [{}] was not found. skipping alerts loading...", ALERT_INDEX);
            templateUtils.ensureIndexTemplateIsLoaded(state, ALERT_INDEX_TEMPLATE);
            started.set(true);
            return true;
        }

        if (state.routingTable().index(ALERT_INDEX).allPrimaryShardsActive()) {
            logger.debug("alerts index [{}] found with all active primary shards. loading alerts...", ALERT_INDEX);
            try {
                int count = loadAlerts(client, scrollSize, scrollTimeout, alertIndexMetaData.numberOfShards(), alertParser, alertMap);
                logger.info("loaded [{}] alerts from the alert index [{}]", count, ALERT_INDEX);
            } catch (Exception e) {
                logger.warn("failed to load alerts for alert index [{}]. scheduled to retry alert loading...", e, ALERT_INDEX);
                alertMap.clear();
                return false;
            }
            templateUtils.ensureIndexTemplateIsLoaded(state, ALERT_INDEX_TEMPLATE);
            started.set(true);
            return true;
        }
        logger.warn("not all primary shards of the alerts index [{}] are started. scheduled to retry alert loading...", ALERT_INDEX);
        return false;
    }

    public boolean started() {
        return started.get();
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            alertMap.clear();
            logger.info("stopped alerts store");
        }
    }

    /**
     * Returns the alert with the specified name otherwise <code>null</code> is returned.
     */
    public Alert getAlert(String name) {
        ensureStarted();
        return alertMap.get(name);
    }

    /**
     * Creates an alert with the specified name and source. If an alert with the specified name already exists it will
     * get overwritten.
     */
    public AlertPut putAlert(String alertName, BytesReference alertSource) {
        ensureStarted();
        Alert alert = alertParser.parse(alertName, false, alertSource);
        IndexRequest indexRequest = createIndexRequest(alertName, alertSource);
        IndexResponse response = client.index(indexRequest).actionGet();
        alert.status().version(response.getVersion());
        Alert previous = alertMap.put(alertName, alert);
        return new AlertPut(previous, alert, response);
    }

    /**
     * Updates and persists the status of the given alert
     */
    void updateAlertStatus(Alert alert) throws IOException {
        // at the moment we store the status together with the alert,
        // so we just need to update the alert itself
        // TODO: consider storing the status in a different documment (alert_status doc) (must smaller docs... faster for frequent updates)
        updateAlert(alert);
    }

    /**
     * Updates and persists the given alert
     */
    void updateAlert(Alert alert) throws IOException {
        ensureStarted();
        assert alert == alertMap.get(alert.name()) : "update alert can only be applied to an already loaded alert";
        BytesReference source = JsonXContent.contentBuilder().value(alert).bytes();
        IndexResponse response = client.index(createIndexRequest(alert.name(), source)).actionGet();
        alert.status().version(response.getVersion());
        // Don't need to update the alertMap, since we are working on an instance from it.
    }

    /**
     * Deletes the alert with the specified name if exists
     */
    public AlertDelete deleteAlert(String name) {
        ensureStarted();
        Alert alert = alertMap.remove(name);
        // even if the alert was not found in the alert map, we should still try to delete it
        // from the index, just to make sure we don't leave traces of it
        DeleteRequest request = new DeleteRequest(ALERT_INDEX, ALERT_TYPE, name);
        if (alert != null) {
            request.version(alert.status().version());
        }
        DeleteResponse response = client.delete(request).actionGet();
        return new AlertDelete(response);
    }

    public ConcurrentMap<String, Alert> getAlerts() {
        return alertMap;
    }

    IndexRequest createIndexRequest(String alertName, BytesReference alertSource) {
        IndexRequest indexRequest = new IndexRequest(ALERT_INDEX, ALERT_TYPE, alertName);
        indexRequest.listenerThreaded(false);
        indexRequest.source(alertSource, false);
        return indexRequest;
    }

    /**
     * scrolls all the alert documents in the alerts index, parses them, and loads them into
     * the given map.
     */
    static int loadAlerts(ClientProxy client, int scrollSize, TimeValue scrollTimeout, int numPrimaryShards, Alert.Parser parser, Map<String, Alert> alerts) {
        assert alerts.isEmpty() : "no alerts should reside, but there are [" + alerts.size() + "] alerts.";
        RefreshResponse refreshResponse = client.admin().indices().refresh(new RefreshRequest(ALERT_INDEX)).actionGet();
        if (refreshResponse.getSuccessfulShards() < numPrimaryShards) {
            throw new AlertsException("not all required shards have been refreshed");
        }

        int count = 0;
        SearchResponse response = client.prepareSearch(ALERT_INDEX)
                .setTypes(ALERT_TYPE)
                .setPreference("_primary")
                .setSearchType(SearchType.SCAN)
                .setScroll(scrollTimeout)
                .setSize(scrollSize)
                .setVersion(true)
                .get();
        try {
            if (response.getTotalShards() != response.getSuccessfulShards()) {
                throw new ElasticsearchException("Partial response while loading alerts");
            }

            if (response.getHits().getTotalHits() > 0) {
                response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeout).get();
                while (response.getHits().hits().length != 0) {
                    for (SearchHit hit : response.getHits()) {
                        String name = hit.getId();
                        Alert alert = parser.parse(name, true, hit.getSourceRef());
                        alert.status().version(hit.version());
                        alerts.put(name, alert);
                        count++;
                    }
                    response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeout).get();
                }
            }
        } finally {
            client.prepareClearScroll().addScrollId(response.getScrollId()).get();
        }
        return count;
    }

    private void ensureStarted() {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("Alert store not started");
        }
    }

    public final class AlertPut {

        private final Alert previous;
        private final Alert current;
        private final IndexResponse response;

        public AlertPut(Alert previous, Alert current, IndexResponse response) {
            this.current = current;
            this.previous = previous;
            this.response = response;
        }

        public Alert current() {
            return current;
        }

        public Alert previous() {
            return previous;
        }

        public IndexResponse indexResponse() {
            return response;
        }
    }

    public final class AlertDelete {

        private final DeleteResponse response;

        public AlertDelete(DeleteResponse response) {
            this.response = response;
        }

        public DeleteResponse deleteResponse() {
            return response;
        }
    }

}
