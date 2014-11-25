/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.actions.AlertActionRegistry;
import org.elasticsearch.alerts.triggers.TriggerManager;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 */
public class AlertsStore extends AbstractComponent {

    public static final String ALERT_INDEX = ".alerts";
    public static final String ALERT_TYPE = "alert";

    public static final ParseField SCHEDULE_FIELD = new ParseField("schedule");
    public static final ParseField TRIGGER_FIELD = new ParseField("trigger");
    public static final ParseField ACTION_FIELD = new ParseField("actions");
    public static final ParseField LAST_ACTION_FIRE = new ParseField("last_action_fire");
    public static final ParseField ENABLE = new ParseField("enable");
    public static final ParseField REQUEST_FIELD = new ParseField("request");
    public static final ParseField THROTTLE_PERIOD_FIELD = new ParseField("throttle_period");
    public static final ParseField LAST_ACTION_EXECUTED_FIELD = new ParseField("last_action_executed");
    public static final ParseField ACK_STATE_FIELD = new ParseField("ack_state");

    private final Client client;
    private final TriggerManager triggerManager;
    private final TemplateHelper templateHelper;
    private final ConcurrentMap<String, Alert> alertMap;
    private final AlertActionRegistry alertActionRegistry;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final int scrollSize;
    private final TimeValue scrollTimeout;

    @Inject
    public AlertsStore(Settings settings, Client client, AlertActionRegistry alertActionRegistry,
                       TriggerManager triggerManager, TemplateHelper templateHelper) {
        super(settings);
        this.client = client;
        this.alertActionRegistry = alertActionRegistry;
        this.templateHelper = templateHelper;
        this.alertMap = ConcurrentCollections.newConcurrentMap();
        // Not using component settings, to let AlertsStore and AlertActionManager share the same settings
        this.scrollSize = settings.getAsInt("alerts.scroll.size", 100);
        this.scrollTimeout = settings.getAsTime("alerts.scroll.timeout", TimeValue.timeValueSeconds(30));
        this.triggerManager = triggerManager;
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
        Alert alert = parseAlert(alertName, alertSource);
        IndexRequest indexRequest = createIndexRequest(alertName, alertSource);
        IndexResponse response = client.index(indexRequest).actionGet();
        alert.version(response.getVersion());
        Alert previous = alertMap.put(alertName, alert);
        return new AlertStoreModification(previous, alert, response);
    }

    /**
     * Updates the specified alert by making sure that the made changes are persisted.
     */
    public void updateAlert(Alert alert) throws IOException {
        ensureStarted();
        // TODO: the content type should be based on the provided content type when the alert was initially added.
        BytesReference source = jsonBuilder().value(alert).bytes();
        IndexResponse response = client.index(createIndexRequest(alert.alertName(), source)).actionGet();
        alert.version(response.getVersion());
        // Don't need to update the alertMap, since we are working on an instance from it.
        assert verifySameInstance(alert);
    }

    private boolean verifySameInstance(Alert alert) {
        Alert found = alertMap.get(alert.alertName());
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
        deleteRequest.version(alert.version());
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
                    loadAlerts();
                } catch (Exception e) {
                    logger.warn("Failed to load alerts", e);
                }
                templateHelper.checkAndUploadIndexTemplate(state, "alerts");
                started.set(true);
                return true;
            } else {
                logger.info("Not all primary shards of the .alerts index are started");
                return false;
            }
        } else {
            logger.info("No previous .alert index, skip loading of alerts");
            templateHelper.checkAndUploadIndexTemplate(state, "alerts");
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

    private void loadAlerts() {
        client.admin().indices().refresh(new RefreshRequest(ALERT_INDEX)).actionGet();

        SearchResponse response = client.prepareSearch(ALERT_INDEX)
                .setTypes(ALERT_TYPE)
                .setSearchType(SearchType.SCAN)
                .setScroll(scrollTimeout)
                .setSize(scrollSize)
                .setVersion(true)
                .get();
        try {
            if (response.getHits().getTotalHits() > 0) {
                response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeout).get();
                while (response.getHits().hits().length != 0) {
                    for (SearchHit sh : response.getHits()) {
                        String alertId = sh.getId();
                        Alert alert = parseAlert(alertId, sh);
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

    private Alert parseAlert(String alertId, SearchHit sh) {
        Alert alert = parseAlert(alertId, sh.getSourceRef());
        alert.version(sh.version());
        return alert;
    }

    protected Alert parseAlert(String alertName, BytesReference source) {
        Alert alert = new Alert();
        alert.alertName(alertName);
        try (XContentParser parser = XContentHelper.createParser(source)) {
            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            assert token == XContentParser.Token.START_OBJECT;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (TRIGGER_FIELD.match(currentFieldName)) {
                        alert.trigger(triggerManager.instantiateAlertTrigger(parser));
                    } else if (ACTION_FIELD.match(currentFieldName)) {
                        List<AlertAction> actions = alertActionRegistry.instantiateAlertActions(parser);
                        alert.actions(actions);
                    } else if (REQUEST_FIELD.match(currentFieldName)) {
                        alert.setSearchRequest(AlertUtils.readSearchRequest(parser));
                    } else {
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    if (SCHEDULE_FIELD.match(currentFieldName)) {
                        alert.schedule(parser.textOrNull());
                    } else if (ENABLE.match(currentFieldName)) {
                        alert.enabled(parser.booleanValue());
                    } else if (LAST_ACTION_FIRE.match(currentFieldName)) {
                        alert.lastActionFire(DateTime.parse(parser.textOrNull()));
                    } else if (LAST_ACTION_EXECUTED_FIELD.match(currentFieldName)) {
                        alert.setTimeLastActionExecuted(DateTime.parse(parser.textOrNull()));
                    } else if (THROTTLE_PERIOD_FIELD.match(currentFieldName)) {
                        alert.setThrottlePeriod(TimeValue.parseTimeValue(parser.textOrNull(), new TimeValue(0)));
                    } else if (ACK_STATE_FIELD.match(currentFieldName)) {
                        alert.setAckState(AlertAckState.fromString(parser.textOrNull()));
                    } else {
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "]");
                }
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Error during parsing alert", e);
        }

        if (alert.lastActionFire() == null) {
            alert.lastActionFire(new DateTime(0));
        }

        if (alert.schedule() == null) {
            throw new ElasticsearchIllegalArgumentException("Schedule is a required field");
        }

        if (alert.trigger() == null) {
            throw new ElasticsearchIllegalArgumentException("Trigger is a required field");
        }

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

        public Alert getCurrent() {
            return current;
        }

        public Alert getPrevious() {
            return previous;
        }

        public IndexResponse getIndexResponse() {
            return indexResponse;
        }
    }

}
