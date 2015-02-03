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
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class AlertsStore extends AbstractComponent {

    public static final String ALERT_INDEX = ".alerts";
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
    public AlertStoreModification putAlert(String alertName, BytesReference alertSource) {
        ensureStarted();
        Alert alert = alertParser.parse(alertName, false, alertSource);
        IndexRequest indexRequest = createIndexRequest(alertName, alertSource);
        IndexResponse response = client.index(indexRequest).actionGet();
        alert.status().version(response.getVersion());
        Alert previous = alertMap.put(alertName, alert);
        return new AlertStoreModification(previous, alert, response);
    }

    /**
     * Updates the specified alert by making sure that the made changes are persisted.
     */
    public void updateAlertStatus(Alert alert) throws IOException {
        updateAlert(alert);
    }

    /**
     * Updates the specified alert by making sure that the made changes are persisted.
     */
    public void updateAlert(Alert alert) throws IOException {
        ensureStarted();
        BytesReference source = XContentFactory.contentBuilder(XContentType.JSON).value(alert).bytes();
        IndexResponse response = client.index(createIndexRequest(alert.name(), source)).actionGet();
        alert.status().version(response.getVersion());
        // Don't need to update the alertMap, since we are working on an instance from it.
        assert verifySameInstance(alert);
    }

    private boolean verifySameInstance(Alert alert) {
        Alert found = alertMap.get(alert.name());
        assert found == alert : "expected " + alert + " but got " + found;
        return true;
    }

    /**
     * Deletes the alert with the specified name if exists
     */
    public DeleteResponse deleteAlert(String name) {
        ensureStarted();
        Alert alert = alertMap.remove(name);
        if (alert == null) {
            return new DeleteResponse(ALERT_INDEX, ALERT_TYPE, name, Versions.MATCH_ANY, false);
        }

        DeleteRequest deleteRequest = new DeleteRequest(ALERT_INDEX, ALERT_TYPE, name);
        deleteRequest.version(alert.status().version());
        DeleteResponse deleteResponse = client.delete(deleteRequest).actionGet();
        assert deleteResponse.isFound();
        return deleteResponse;
    }

    public ConcurrentMap<String, Alert> getAlerts() {
        return alertMap;
    }

    public boolean start(ClusterState state) {
        if (started.get()) {
            return true;
        }

        IndexMetaData alertIndexMetaData = state.getMetaData().index(ALERT_INDEX);
        if (alertIndexMetaData != null) {
            logger.debug("Previous alerting index");
            if (state.routingTable().index(ALERT_INDEX).allPrimaryShardsActive()) {
                logger.debug("Previous alerting index with active primary shards");
                try {
                    loadAlerts(alertIndexMetaData.numberOfShards());
                } catch (Exception e) {
                    logger.warn("Failed to load previously stored alerts. Schedule to retry alert loading...", e);
                    alertMap.clear();
                    return false;
                }
                templateUtils.checkAndUploadIndexTemplate(state, "alerts");
                started.set(true);
                return true;
            } else {
                logger.warn("Not all primary shards of the .alerts index are started. Schedule to retry alert loading...");
                return false;
            }
        } else {
            logger.info("No previous .alert index, skip loading of alerts");
            templateUtils.checkAndUploadIndexTemplate(state, "alerts");
            started.set(true);
            return true;
        }
    }

    public boolean started() {
        return started.get();
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            alertMap.clear();
            logger.info("Stopped alert store");
        }
    }

    private IndexRequest createIndexRequest(String alertName, BytesReference alertSource) {
        IndexRequest indexRequest = new IndexRequest(ALERT_INDEX, ALERT_TYPE, alertName);
        indexRequest.listenerThreaded(false);
        indexRequest.source(alertSource, false);
        return indexRequest;
    }

    private void loadAlerts(int numPrimaryShards) {
        assert alertMap.isEmpty() : "No alerts should reside, but there are " + alertMap.size() + " alerts.";
        RefreshResponse refreshResponse = client.admin().indices().refresh(new RefreshRequest(ALERT_INDEX)).actionGet();
        if (refreshResponse.getSuccessfulShards() < numPrimaryShards) {
            throw new ElasticsearchException("Not all required shards have been refreshed");
        }

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
                    for (SearchHit sh : response.getHits()) {
                        String alertId = sh.getId();
                        Alert alert = parseLoadedAlert(alertId, sh);
                        alertMap.put(alertId, alert);
                    }
                    response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeout).get();
                }
            }
        } finally {
            client.prepareClearScroll().addScrollId(response.getScrollId()).get();
        }
        logger.info("Loaded [{}] alerts from the alert index.", alertMap.size());
    }

    private Alert parseLoadedAlert(String alertId, SearchHit sh) {
        Alert alert = alertParser.parse(alertId, true, sh.getSourceRef());
        alert.status().version(sh.version());
        return alert;
    }

    private void ensureStarted() {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("Alert store not started");
        }
    }

    public final class AlertStoreModification {

        private final Alert previous;
        private final Alert current;
        private final IndexResponse indexResponse;

        public AlertStoreModification(Alert previous, Alert current, IndexResponse indexResponse) {
            this.current = current;
            this.previous = previous;
            this.indexResponse = indexResponse;
        }

        public Alert current() {
            return current;
        }

        public Alert previous() {
            return previous;
        }

        public IndexResponse indexResponse() {
            return indexResponse;
        }
    }

}
