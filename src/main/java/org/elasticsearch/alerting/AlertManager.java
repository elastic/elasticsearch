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
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;


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
    public final ParseField INDICES = new ParseField("indices");

    private final Client client;
    private AlertScheduler scheduler;

    private final Map<String,Alert> alertMap;

    private AtomicBoolean started = new AtomicBoolean(false);
    private final Thread starter;

    private AlertActionManager actionManager;

    class StarterThread implements Runnable {
        @Override
        public void run() {
            logger.warn("Starting thread to get alerts");
            int attempts = 0;
            while (attempts < 2) {
                try {
                    logger.warn("Sleeping [{}]", attempts);
                    Thread.sleep(10000);
                    logger.warn("Slept");
                    break;
                } catch (InterruptedException ie) {
                    ++attempts;
                }
            }
            logger.warn("Loading alerts");
            try {
                loadAlerts();
                started.set(true);
            } catch (Throwable t) {
                logger.error("Failed to load alerts", t);
            }
            //Build the mapping for the scheduler
            Map<String,String> alertNameToSchedule = new HashMap();
            synchronized (alertMap) {
                for (Map.Entry<String, Alert> entry : alertMap.entrySet()) {
                    scheduler.addAlert(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    @Inject
    public void setActionManager(AlertActionManager actionManager){
        this.actionManager = actionManager;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.warn("STARTING");
        starter.start();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.warn("STOPPING");
        /*
        try {
            starter.join();
        } catch (InterruptedException ie) {
            logger.warn("Interrupted on joining start thread.", ie);
        }
        */
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.warn("CLOSING");
    }


    @Inject
    public AlertManager(Settings settings, Client client) {
        super(settings);
        logger.warn("Initing AlertManager");
        this.client = client;
        alertMap = new HashMap();
        starter = new Thread(new StarterThread());
        //scheduleAlerts();
    }

    @Inject
    public void setAlertScheduler(AlertScheduler scheduler){
        this.scheduler = scheduler;
    }

    private ClusterHealthStatus createAlertsIndex() throws InterruptedException, ExecutionException {
        CreateIndexResponse cir = client.admin().indices().prepareCreate(ALERT_INDEX).addMapping(ALERT_TYPE).execute().get(); //TODO FIX MAPPINGS
        logger.warn(cir.toString());
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
                    "{ \"query\" : " +
                            "{ \"match_all\" :  {}}," +
                            "\"size\" : \"100\"" +
                    "}"
            ).setTypes(ALERT_TYPE).setIndices(ALERT_INDEX).execute().get();
            for (SearchHit sh : searchResponse.getHits()) {
                String alertId = sh.getId();
                logger.warn("Found : [{}]", alertId);
                Map<String,Object> fields = sh.sourceAsMap();
                //Map<String,SearchHitField> fields = sh.getFields();

                for (String field : fields.keySet() ) {
                    logger.warn("Field : [{}]", field);
                }

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
                TimeValue timePeriod = new TimeValue(Long.valueOf(fields.get(TIMEPERIOD_FIELD.getPreferredName()).toString()));

                Object actionObj = fields.get(ACTION_FIELD.getPreferredName());
                List<AlertAction> actions = null;
                if (actionObj instanceof Map) {
                    Map<String, Object> actionMap = (Map<String, Object>) actionObj;
                    actions = actionManager.parseActionsFromMap(actionMap);
                } else {
                    throw new ElasticsearchException("Unable to parse actions [" + triggerObj + "]");
                }

                DateTime lastRan = new DateTime(fields.get("lastRan").toString());

                List<String> indices = null;
                if (fields.get(INDICES.getPreferredName()) != null && fields.get(INDICES.getPreferredName()) instanceof List){
                    indices = (List<String>)fields.get(INDICES.getPreferredName());
                }

                Alert alert = new Alert(alertId, query, trigger, timePeriod, actions, schedule, lastRan, indices);
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
