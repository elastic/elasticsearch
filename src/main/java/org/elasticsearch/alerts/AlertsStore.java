/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
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
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class AlertsStore extends AbstractComponent {

    public static final ParseField QUERY_NAME_FIELD =  new ParseField("query");
    public static final ParseField SCHEDULE_FIELD =  new ParseField("schedule");
    public static final ParseField TRIGGER_FIELD = new ParseField("trigger");
    public static final ParseField TIMEPERIOD_FIELD = new ParseField("timeperiod");
    public static final ParseField ACTION_FIELD = new ParseField("action");
    public static final ParseField LASTRAN_FIELD = new ParseField("lastRan");
    public static final ParseField INDICES = new ParseField("indices");
    public static final ParseField CURRENTLY_RUNNING = new ParseField("running");
    public static final ParseField ENABLED = new ParseField("enabled");
    public static final ParseField SIMPLE_QUERY = new ParseField("simple");
    public static final ParseField TIMESTAMP_FIELD = new ParseField("timefield");
    public static final ParseField LAST_ACTION_FIRE = new ParseField("lastactionfire");

    private final TimeValue defaultTimePeriod = new TimeValue(300*1000); //TODO : read from config

    private final Client client;
    private final AlertActionRegistry alertActionRegistry;
    private final ConcurrentMap<String,Alert> alertMap;

    private final int scrollSize;
    private final TimeValue scrollTimeout;

    @Inject
    public AlertsStore(Settings settings, Client client, AlertActionRegistry alertActionRegistry) {
        super(settings);
        this.client = client;
        this.alertActionRegistry = alertActionRegistry;
        this.alertMap = ConcurrentCollections.newConcurrentMap();
        this.scrollSize = componentSettings.getAsInt("scroll.size", 100);
        this.scrollTimeout = componentSettings.getAsTime("scroll.timeout", TimeValue.timeValueSeconds(30));
    }

    /**
     * Returns whether an alert with the specified name exists.
     */
    public boolean hasAlert(String name) {
        return alertMap.containsKey(name);
    }

    /**
     * Returns the alert with the specified name otherwise <code>null</code> is returned.
     */
    public Alert getAlert(String name) {
        return alertMap.get(name);
    }

    /**
     * Creates an alert with the specified and fails if an alert with the name already exists.
     */
    public Alert createAlert(String name, BytesReference alertSource) {
        if (!client.admin().indices().prepareExists(AlertManager.ALERT_INDEX).execute().actionGet().isExists()) {
            createAlertsIndex();
        }

        Alert alert = parseAlert(name, alertSource, 1);
        if (alertMap.putIfAbsent(name, alert) == null) {
            persistAlert(name, alertSource, IndexRequest.OpType.CREATE);
        } else {
            throw new ElasticsearchIllegalArgumentException("There is already an alert named [" + name + "]");
        }
        return alert;
    }

    /**
     * Updates the specified alert by making sure that the made changes are persisted.
     */
    public void updateAlert(Alert alert) {
        IndexRequest updateRequest = new IndexRequest();
        updateRequest.index(AlertManager.ALERT_INDEX);
        updateRequest.type(AlertManager.ALERT_TYPE);
        updateRequest.id(alert.alertName());
        updateRequest.version(alert.version());
        XContentBuilder alertBuilder;
        try {
            alertBuilder = XContentFactory.jsonBuilder();
            alert.toXContent(alertBuilder, ToXContent.EMPTY_PARAMS);
        } catch (IOException ie) {
            throw new ElasticsearchException("Unable to serialize alert ["+ alert.alertName() + "]", ie);
        }
        updateRequest.source(alertBuilder);

        IndexResponse response = client.index(updateRequest).actionGet();
        alert.version(response.getVersion());

        // Don't need to update the alertMap, since we are working on an instance from it.
        assert alertMap.get(alert.alertName()) == alert;
    }

    /**
     * Deletes the alert with the specified name if exists
     */
    public void deleteAlert(String name) {
        Alert alert = alertMap.remove(name);
        if (alert != null) {
            DeleteRequest deleteRequest = new DeleteRequest();
            deleteRequest.id(name);
            deleteRequest.index(AlertManager.ALERT_INDEX);
            deleteRequest.type(AlertManager.ALERT_TYPE);
            deleteRequest.version(alert.version());
            DeleteResponse deleteResponse = client.delete(deleteRequest).actionGet();
            assert deleteResponse.isFound();
        }
    }

    /**
     * Clears the in-memory representation of the alerts and loads the alerts from the .alerts index.
     */
    public void reload() {
        clear();
        loadAlerts();
    }

    /**
     * Clears the in-memory representation of the alerts
     */
    public void clear() {
        alertMap.clear();
    }

    public ConcurrentMap<String, Alert> getAlerts() {
        return alertMap;
    }

    private void persistAlert(String alertName, BytesReference alertSource, IndexRequest.OpType opType) {
        IndexRequest indexRequest = new IndexRequest(AlertManager.ALERT_INDEX, AlertManager.ALERT_TYPE, alertName);
        indexRequest.listenerThreaded(false);
        indexRequest.source(alertSource, false);
        indexRequest.opType(opType);
        client.index(indexRequest).actionGet();
    }

    private void loadAlerts() {
        if (!client.admin().indices().prepareExists(AlertManager.ALERT_INDEX).execute().actionGet().isExists()) {
            createAlertsIndex();
        }

        SearchResponse response = client.prepareSearch()
                .setSearchType(SearchType.SCAN)
                .setScroll(scrollTimeout)
                .setSize(scrollSize)
                .setTypes(AlertManager.ALERT_TYPE)
                .setIndices(AlertManager.ALERT_INDEX).get();
        try {
            for (; response.getHits().hits().length != 0; response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeout).get()) {
                for (SearchHit sh : response.getHits()) {
                    String alertId = sh.getId();
                    Alert alert = parseAlert(alertId, sh);
                    alertMap.put(alertId, alert);
                }
                response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeout).get();
            }
        } finally {
            client.prepareClearScroll().addScrollId(response.getScrollId()).get();
        }
        logger.info("Loaded [{}] alerts from the alert index.", alertMap.size());
    }

    private Alert parseAlert(String alertId, SearchHit sh) {
        return parseAlert(alertId, sh.getSourceRef(), sh.getVersion());
    }

    private Alert parseAlert(String alertName, BytesReference source, long version) {
        Alert alert = new Alert();
        alert.alertName(alertName);
        alert.version(version);
        try (XContentParser parser = XContentHelper.createParser(source)) {
            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            assert token == XContentParser.Token.START_OBJECT;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (TRIGGER_FIELD.match(currentFieldName)) {
                        alert.trigger(TriggerManager.parseTrigger(parser));
                    } else if (ACTION_FIELD.match(currentFieldName)) {
                        List<AlertAction> actions = alertActionRegistry.instantiateAlertActions(parser);
                        alert.actions(actions);
                    } else {
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (INDICES.match(currentFieldName)) {
                        List<String> indices = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            indices.add(parser.text());
                        }
                        alert.indices(indices);
                    } else {
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    if (QUERY_NAME_FIELD.match(currentFieldName)) {
                        alert.queryName(parser.textOrNull());
                    } else if (SCHEDULE_FIELD.match(currentFieldName)) {
                        alert.schedule(parser.textOrNull());
                    } else if (TIMEPERIOD_FIELD.match(currentFieldName)) {
                        alert.timestampString(parser.textOrNull());
                    } else if (LASTRAN_FIELD.match(currentFieldName)) {
                        alert.lastRan(DateTime.parse(parser.textOrNull()));
                    } else if (CURRENTLY_RUNNING.match(currentFieldName)) {
                        alert.running(DateTime.parse(parser.textOrNull()));
                    } else if (ENABLED.match(currentFieldName)) {
                        alert.enabled(parser.booleanValue());
                    } else if (SIMPLE_QUERY.match(currentFieldName)) {
                        alert.simpleQuery(parser.booleanValue());
                    } else if (TIMEPERIOD_FIELD.match(currentFieldName)) {
                        alert.timePeriod(TimeValue.parseTimeValue(parser.textOrNull(), defaultTimePeriod));
                    } else if (LAST_ACTION_FIRE.match(currentFieldName)) {
                        alert.lastActionFire(DateTime.parse(parser.textOrNull()));
                    } else if (TIMESTAMP_FIELD.match(currentFieldName)) {
                        alert.timestampString(parser.textOrNull());
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

        if (alert.timePeriod() == null) {
            alert.timePeriod(defaultTimePeriod);
        }
        if (alert.lastActionFire() == null) {
            alert.lastActionFire(new DateTime(0));
        }
        return alert;
    }

    private ClusterHealthStatus createAlertsIndex() {
        CreateIndexResponse cir = client.admin().indices().prepareCreate(AlertManager.ALERT_INDEX).addMapping(AlertManager.ALERT_TYPE).execute().actionGet(); //TODO FIX MAPPINGS
        ClusterHealthResponse actionGet = client.admin().cluster()
                .health(Requests.clusterHealthRequest(AlertManager.ALERT_INDEX).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        return actionGet.getStatus();
    }

}
