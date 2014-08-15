/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;


public class AlertManager extends AbstractLifecycleComponent {

    public final String ALERT_INDEX = ".alerts";
    public final String ALERT_TYPE = "alert";
    public final String ALERT_HISTORY_TYPE = "alertHistory";

    public static final ParseField QUERY_FIELD =  new ParseField("query");
    public static final ParseField SCHEDULE_FIELD =  new ParseField("schedule");
    public static final ParseField TRIGGER_FIELD = new ParseField("trigger");
    public static final ParseField TIMEPERIOD_FIELD = new ParseField("timeperiod");
    public static final ParseField ACTION_FIELD = new ParseField("action");
    public static final ParseField LASTRAN_FIELD = new ParseField("lastRan");
    public static final ParseField INDICES = new ParseField("indices");

    private final Client client;
    private AlertScheduler scheduler;

    private final Map<String,Alert> alertMap;

    private AtomicBoolean started = new AtomicBoolean(false);
    private final Thread starter;

    private AlertActionManager actionManager;
    final TimeValue defaultTimePeriod = new TimeValue(300*1000); //TODO : read from config

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
                refreshAlerts();
                started.set(true);
            } catch (Throwable t) {
                logger.error("Failed to load alerts", t);
            }
        }
    }

    private void sendAlertsToScheduler() {
        synchronized (alertMap) {
            for (Map.Entry<String, Alert> entry : alertMap.entrySet()) {
                scheduler.addAlert(entry.getKey(), entry.getValue());
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

    public void refreshAlerts() {
        try {
            synchronized (alertMap) {
                scheduler.clearAlerts();
                alertMap.clear();
                loadAlerts();
                sendAlertsToScheduler();
            }
        } catch (Exception e){
            throw new ElasticsearchException("Failed to refresh alerts",e);
        }
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
                Alert alert = parseAlert(alertId, sh);
                alertMap.put(alertId, alert);
            }
            logger.warn("Loaded [{}] alerts from the alert index.", alertMap.size());
        }
    }

    public long getLastEventCount(String alertName){
        return 0;
    }

    public boolean updateLastRan(String alertName, DateTime fireTime) throws Exception {
        try {
            synchronized (alertMap) {
                Alert alert = getAlertForName(alertName);
                alert.lastRan(fireTime);
                XContentBuilder alertBuilder = XContentFactory.jsonBuilder().prettyPrint();
                alert.toXContent(alertBuilder);
                UpdateRequest updateRequest = new UpdateRequest();
                updateRequest.id(alertName);
                updateRequest.index(ALERT_INDEX);
                updateRequest.type(ALERT_TYPE);
                updateRequest.doc(alertBuilder);
                updateRequest.refresh(true);
                client.update(updateRequest).actionGet();
                return true;
            }
        } catch (Throwable t) {
            logger.error("Failed to update alert [{}] with lastRan of [{}]",t, alertName, fireTime);
            return false;
        }
    }

    public boolean addHistory(String alertName, boolean triggered,
                              DateTime fireTime, XContentBuilder triggeringQuery,
                              AlertTrigger trigger, long numberOfResults,
                              @Nullable List<String> indices) throws Exception {
        XContentBuilder historyEntry = XContentFactory.jsonBuilder();
        historyEntry.startObject();
        historyEntry.field("alertName", alertName);
        historyEntry.field("triggered", triggered);
        historyEntry.field("fireTime", fireTime.toDateTimeISO());
        historyEntry.field("trigger");
        trigger.toXContent(historyEntry);
        historyEntry.field("queryRan", XContentHelper.convertToJson(triggeringQuery.bytes(),false,true));
        historyEntry.field("numberOfResults", numberOfResults);
        if (indices != null) {
            historyEntry.field("indices");
            historyEntry.startArray();
            for (String index : indices) {
                historyEntry.value(index);
            }
            historyEntry.endArray();
        }
        historyEntry.endObject();
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(ALERT_INDEX);
        indexRequest.type(ALERT_HISTORY_TYPE);
        indexRequest.source(historyEntry);
        indexRequest.listenerThreaded(false);
        indexRequest.operationThreaded(false);
        indexRequest.refresh(true); //Always refresh after indexing an alert
        indexRequest.opType(IndexRequest.OpType.CREATE);
        return client.index(indexRequest).get().isCreated();
    }

    public boolean deleteAlert(String alertName) throws InterruptedException, ExecutionException{
        synchronized (alertMap) {
            if (alertMap.containsKey(alertName)) {
                scheduler.deleteAlertFromSchedule(alertName);
                alertMap.remove(alertName);
                try {
                    DeleteRequest deleteRequest = new DeleteRequest();
                    deleteRequest.id(alertName);
                    deleteRequest.index(ALERT_INDEX);
                    deleteRequest.type(ALERT_TYPE);
                    deleteRequest.operationThreaded(false);
                    deleteRequest.refresh(true);
                    if (client.delete(deleteRequest).actionGet().isFound()) {
                        return true;
                    } else {
                        logger.warn("Couldn't find [{}] in the index triggering a full refresh", alertName);
                        //Something went wrong refresh
                        refreshAlerts();
                        return false;
                    }
                }
                catch (Exception e){
                    logger.warn("Something went wrong when deleting [{}] from the index triggering a full refresh", e, alertName);
                    //Something went wrong refresh
                    refreshAlerts();
                    throw e;
                }
            }
        }
        return false;
    }

    public boolean addAlert(String alertName, Alert alert, boolean persist) throws InterruptedException, ExecutionException{
        synchronized (alertMap) {
            if (alertMap.containsKey(alertName)) {
                throw new ElasticsearchIllegalArgumentException("There is already an alert named ["+alertName+"]");
            } else {
                alertMap.put(alertName, alert);
                scheduler.addAlert(alertName,alert);
                if (persist) {
                    XContentBuilder builder;
                    try {
                        builder = XContentFactory.jsonBuilder();
                        alert.toXContent(builder);
                        IndexRequest indexRequest = new IndexRequest(ALERT_INDEX, ALERT_TYPE, alertName);
                        indexRequest.listenerThreaded(false);
                        indexRequest.operationThreaded(false);
                        indexRequest.refresh(true); //Always refresh after indexing an alert
                        indexRequest.source(builder);
                        indexRequest.opType(IndexRequest.OpType.CREATE);
                        return client.index(indexRequest).get().isCreated();
                    } catch (IOException ie) {
                        throw new ElasticsearchIllegalStateException("Unable to convert alert to JSON", ie);
                    }
                } else {
                    return true;
                }
            }
        }
    }

    private Alert parseAlert(String alertId, SearchHit sh) {

        Map<String, Object> fields = sh.sourceAsMap();
        return parseAlert(alertId,fields);
    }

    public Alert parseAlert(String alertId, Map<String, Object> fields) {
        //Map<String,SearchHitField> fields = sh.getFields();
        logger.warn("Parsing : [{}]", alertId);
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

        String timeString = fields.get(TIMEPERIOD_FIELD.getPreferredName()).toString();
        TimeValue timePeriod = TimeValue.parseTimeValue(timeString, defaultTimePeriod);

        Object actionObj = fields.get(ACTION_FIELD.getPreferredName());
        List<AlertAction> actions = null;
        if (actionObj instanceof Map) {
            Map<String, Object> actionMap = (Map<String, Object>) actionObj;
            actions = actionManager.parseActionsFromMap(actionMap);
        } else {
            throw new ElasticsearchException("Unable to parse actions [" + triggerObj + "]");
        }

        DateTime lastRan = new DateTime(0);
        if( fields.get(LASTRAN_FIELD.getPreferredName()) != null){
            lastRan = new DateTime(fields.get(LASTRAN_FIELD.getPreferredName()).toString());
        } else if (fields.get("lastRan") != null) {
            lastRan = new DateTime(fields.get("lastRan").toString());
        }

        List<String> indices = new ArrayList<>();
        if (fields.get(INDICES.getPreferredName()) != null && fields.get(INDICES.getPreferredName()) instanceof List){
            indices = (List<String>)fields.get(INDICES.getPreferredName());
        } else {
            logger.warn("Indices : " + fields.get(INDICES.getPreferredName()) + " class " + fields.get(INDICES.getPreferredName()).getClass() );
        }

        return new Alert(alertId, query, trigger, timePeriod, actions, schedule, lastRan, indices);
    }

    public Map<String,Alert> getSafeAlertMap() {
        synchronized (alertMap) {
            return new HashMap<>(alertMap);
        }
    }

    public Alert getAlertForName(String alertName) {
        synchronized (alertMap) {
            return alertMap.get(alertName);
        }
    }

}
