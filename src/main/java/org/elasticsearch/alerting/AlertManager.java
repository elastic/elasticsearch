/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


public class AlertManager extends AbstractLifecycleComponent {

    public final String ALERT_INDEX = ".alerts";
    public final String ALERT_TYPE = "alert";
    public final String QUERY_TYPE = "alertQuery";

    public final ParseField QUERY_FIELD =  new ParseField("query");
    public final ParseField SCHEDULE_FIELD =  new ParseField("schedule");
    public final ParseField TRIGGER_FIELD = new ParseField("trigger");
    public final ParseField TIMEPERIOD_FIELD = new ParseField("timeperiod");
    public final ParseField ACTION_FIELD = new ParseField("action");
    public final ParseField LASTRAN_FIELD = new ParseField("lastRan");

    public final Client client;

    private final Map<String,Alert> alertMap;

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.warn("STARTING");
        try {
            loadAlerts();
        } catch (Throwable t){
            logger.error("Failed to load alerts", t);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.warn("STOPPING");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.warn("CLOSING");
    }


    @Inject
    public AlertManager(Settings settings, Client client) {
        super(settings);
        logger.warn("Starting AlertManager");
        this.client = client;
        alertMap = new HashMap();
        //scheduleAlerts();
    }

    private ClusterHealthStatus clusterHealth() throws InterruptedException, ExecutionException {
        ClusterHealthResponse actionGet = client.admin().cluster()
                .health(Requests.clusterHealthRequest(ALERT_INDEX).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        return actionGet.getStatus();
    }

    private ClusterHealthStatus createAlertsIndex() throws InterruptedException, ExecutionException {
        client.admin().indices().prepareCreate(ALERT_INDEX).addMapping(ALERT_TYPE).execute().get(); //TODO FIX MAPPINGS
        ClusterHealthResponse actionGet = client.admin().cluster()
                .health(Requests.clusterHealthRequest(ALERT_INDEX).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        return actionGet.getStatus();
    }

    private void loadAlerts() throws InterruptedException, ExecutionException{
        if (!client.admin().indices().prepareExists(ALERT_INDEX).execute().get().isExists()) {
            createAlertsIndex();
        }

        synchronized (alertMap) {
            SearchResponse searchResponse = client.prepareSearch().setSource(
                    "{ 'query' : " +
                            "{ 'match_all' :  {}}" +
                            "'size' : 100" +
                    "}"
            ).setTypes(ALERT_TYPE).setIndices(ALERT_INDEX).execute().get();
            for (SearchHit sh : searchResponse.getHits()) {
                String alertId = sh.getId();
                Map<String,SearchHitField> fields = sh.getFields();
                String query = fields.get(QUERY_FIELD.toString()).toString();
                String schedule = fields.get(SCHEDULE_FIELD.toString()).toString();
                AlertTrigger trigger = TriggerManager.parseTriggerFromSearchField(fields.get(TRIGGER_FIELD.toString()));
                TimeValue timePeriod = new TimeValue(Long.valueOf(fields.get(TIMEPERIOD_FIELD).toString()));
                AlertAction action = AlertActionManager.parseActionFromSearchField(fields.get(ACTION_FIELD.toString()));
                DateTime lastRan = new DateTime(fields.get(LASTRAN_FIELD.toString().toString()));

                Alert alert = new Alert(alertId, query, trigger, timePeriod, action, schedule, lastRan);
                alertMap.put(alertId, alert);
            }
            logger.warn("Loaded [{}] alerts from the alert index.", alertMap.size());
        }
    }

    public Alert getAlertForName(String alertName) {
        synchronized (alertMap) {
            return alertMap.get(alertName);
        }
    }

}
