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
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.actions.AlertActionRegistry;
import org.elasticsearch.alerts.triggers.AlertTrigger;
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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class AlertsStore extends AbstractComponent {

    public static final ParseField QUERY_FIELD =  new ParseField("query");
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

    @Inject
    public AlertsStore(Settings settings, Client client, AlertActionRegistry alertActionRegistry) {
        super(settings);
        this.client = client;
        this.alertActionRegistry = alertActionRegistry;
        alertMap = ConcurrentCollections.newConcurrentMap();
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

        SearchResponse searchResponse = client.prepareSearch().setSource(
                "{ \"query\" : " +
                        "{ \"match_all\" :  {}}," +
                        "\"size\" : \"100\"" +
                        "}"
        ).setTypes(AlertManager.ALERT_TYPE).setIndices(AlertManager.ALERT_INDEX).execute().actionGet();
        for (SearchHit sh : searchResponse.getHits()) {
            String alertId = sh.getId();
            try {
                Alert alert = parseAlert(alertId, sh);
                alertMap.put(alertId, alert);
            } catch (ElasticsearchException e) {
                logger.error("Unable to parse [{}] as an alert this alert will be skipped.",e,sh);
            }
        }
        logger.warn("Loaded [{}] alerts from the alert index.", alertMap.size());
    }

    private Alert parseAlert(String alertId, SearchHit sh) {
        return parseAlert(alertId, sh.getSourceRef(), sh.getVersion());
    }

    private Alert parseAlert(String alertId, BytesReference bytesReference, long version) {
        // TODO: streaming parsing!
        Map<String, Object> fields = XContentHelper.convertToMap(bytesReference, false).v2();
        String query = fields.get(QUERY_FIELD.getPreferredName()).toString();
        String schedule = fields.get(SCHEDULE_FIELD.getPreferredName()).toString();
        Object triggerObj = fields.get(TRIGGER_FIELD.getPreferredName());
        AlertTrigger trigger = null;
        if (triggerObj instanceof Map) {
            Map<String, Object> triggerMap = (Map<String, Object>) triggerObj;
            trigger = TriggerManager.parseTriggerFromMap(triggerMap);
        } else {
            throw new ElasticsearchException("Unable to parse trigger [" + triggerObj + "]");
        }

        String timeString = fields.get(TIMEPERIOD_FIELD.getPreferredName()).toString();
        TimeValue timePeriod = TimeValue.parseTimeValue(timeString, defaultTimePeriod);

        Object actionObj = fields.get(ACTION_FIELD.getPreferredName());
        List<AlertAction> actions = null;
        if (actionObj instanceof Map) {
            Map<String, Object> actionMap = (Map<String, Object>) actionObj;
            actions = alertActionRegistry.parseActionsFromMap(actionMap);
        } else {
            throw new ElasticsearchException("Unable to parse actions [" + actionObj + "]");
        }

        DateTime lastRan = new DateTime(0);
        if( fields.get(LASTRAN_FIELD.getPreferredName()) != null){
            lastRan = new DateTime(fields.get(LASTRAN_FIELD.getPreferredName()).toString());
        } else if (fields.get("lastRan") != null) {
            lastRan = new DateTime(fields.get("lastRan").toString());
        }

        DateTime running = new DateTime(0);
        if (fields.get(CURRENTLY_RUNNING.getPreferredName()) != null) {
            running = new DateTime(fields.get(CURRENTLY_RUNNING.getPreferredName()).toString());
        }

        DateTime lastActionFire = new DateTime(0);
        if (fields.get(LAST_ACTION_FIRE.getPreferredName()) != null) {
            lastActionFire = new DateTime(fields.get(LAST_ACTION_FIRE.getPreferredName()).toString());
        }

        List<String> indices = new ArrayList<>();
        if (fields.get(INDICES.getPreferredName()) != null && fields.get(INDICES.getPreferredName()) instanceof List){
            indices = (List<String>)fields.get(INDICES.getPreferredName());
        } else {
            logger.warn("Indices : " + fields.get(INDICES.getPreferredName()) + " class " + fields.get(INDICES.getPreferredName()).getClass() );
        }

        boolean enabled = true;
        if (fields.get(ENABLED.getPreferredName()) != null ) {
            logger.error(ENABLED.getPreferredName() + " " + fields.get(ENABLED.getPreferredName()));
            Object enabledObj = fields.get(ENABLED.getPreferredName());
            enabled = parseAsBoolean(enabledObj);
        }

        boolean simpleQuery = true;
        if (fields.get(SIMPLE_QUERY.getPreferredName()) != null ) {
            logger.error(SIMPLE_QUERY.getPreferredName() + " " + fields.get(SIMPLE_QUERY.getPreferredName()));
            Object enabledObj = fields.get(SIMPLE_QUERY.getPreferredName());
            simpleQuery = parseAsBoolean(enabledObj);
        }

        Alert alert =  new Alert(alertId, query, trigger, timePeriod, actions, schedule, lastRan, indices, running, version, enabled, simpleQuery);
        alert.lastActionFire(lastActionFire);

        if (fields.get(TIMESTAMP_FIELD.getPreferredName()) != null) {
            alert.timestampString(fields.get(TIMESTAMP_FIELD.getPreferredName()).toString());
        }

        return alert;
    }

    private boolean parseAsBoolean(Object enabledObj) {
        boolean enabled;
        if (enabledObj instanceof Boolean){
            enabled = (Boolean)enabledObj;
        } else {
            if (enabledObj.toString().toLowerCase(Locale.ROOT).equals("true") ||
                    enabledObj.toString().toLowerCase(Locale.ROOT).equals("1")) {
                enabled = true;
            } else if ( enabledObj.toString().toLowerCase(Locale.ROOT).equals("false") ||
                    enabledObj.toString().toLowerCase(Locale.ROOT).equals("0")) {
                enabled = false;
            } else {
                throw new ElasticsearchIllegalArgumentException("Unable to parse [" + enabledObj + "] as a boolean");
            }
        }
        return enabled;
    }

    private ClusterHealthStatus createAlertsIndex() {
        CreateIndexResponse cir = client.admin().indices().prepareCreate(AlertManager.ALERT_INDEX).addMapping(AlertManager.ALERT_TYPE).execute().actionGet(); //TODO FIX MAPPINGS
        ClusterHealthResponse actionGet = client.admin().cluster()
                .health(Requests.clusterHealthRequest(AlertManager.ALERT_INDEX).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        return actionGet.getStatus();
    }

}
