/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.actions.AlertActionRegistry;
import org.elasticsearch.alerts.actions.AlertActionEntry;
import org.elasticsearch.alerts.scheduler.AlertScheduler;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.alerts.triggers.TriggerManager;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
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
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;


public class AlertManager extends AbstractLifecycleComponent {


    public static final String ALERT_INDEX = ".alerts";
    public static final String ALERT_TYPE = "alert";
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

    private final Client client;
    private AlertScheduler scheduler;
    private final ThreadPool threadPool;

    private final ConcurrentMap<String,Alert> alertMap;

    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean startActions = new AtomicBoolean(false);

    private AlertActionRegistry actionRegistry;
    private AlertActionManager actionManager;

    final TimeValue defaultTimePeriod = new TimeValue(300*1000); //TODO : read from config


    private void sendAlertsToScheduler() {
        for (Map.Entry<String, Alert> entry : alertMap.entrySet()) {
            scheduler.addAlert(entry.getKey(), entry.getValue());
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.warn("STARTING");
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
    public AlertManager(Settings settings, Client client, ClusterService clusterService, ThreadPool threadPool,
                        AlertActionRegistry actionRegistry) {
        super(settings);
        logger.warn("Initing AlertManager");
        this.client = client;
        alertMap = ConcurrentCollections.newConcurrentMap();
        clusterService.add(new AlertsClusterStateListener());
        this.threadPool = threadPool;
        this.actionRegistry = actionRegistry;
        this.actionManager = new AlertActionManager(client, this, actionRegistry, threadPool);
    }

    public void setAlertScheduler(AlertScheduler scheduler){
        this.scheduler = scheduler;
    }


    private ClusterHealthStatus createAlertsIndex() {
        CreateIndexResponse cir = client.admin().indices().prepareCreate(ALERT_INDEX).addMapping(ALERT_TYPE).execute().actionGet(); //TODO FIX MAPPINGS
        ClusterHealthResponse actionGet = client.admin().cluster()
                .health(Requests.clusterHealthRequest(ALERT_INDEX).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        return actionGet.getStatus();
    }

    public DateTime timeActionLastTriggered(String alertName) {
        Alert indexedAlert;
        indexedAlert = getAlertFromIndex(alertName);
        if (indexedAlert == null) {
            return null;
        } else {
            return indexedAlert.lastActionFire();
        }
    }


    public void doAction(Alert alert, AlertActionEntry result, DateTime scheduledTime) {
        logger.warn("We have triggered");
        DateTime lastActionFire = timeActionLastTriggered(alert.alertName());
        long msSinceLastAction = scheduledTime.getMillis() - lastActionFire.getMillis();
        logger.error("last action fire [{}]", lastActionFire);
        logger.error("msSinceLastAction [{}]", msSinceLastAction);

        if (alert.timePeriod().getMillis() > msSinceLastAction) {
            logger.warn("Not firing action because it was fired in the timePeriod");
        } else {
            actionRegistry.doAction(alert, result);
            logger.warn("Did action !");

            alert.lastActionFire(scheduledTime);
            persistAlert(alert.alertName(), alert, IndexRequest.OpType.INDEX);
        }
    }

    public boolean claimAlertRun(String alertName, DateTime scheduleRunTime) {
        Alert indexedAlert;
        try {
            indexedAlert = getAlertFromIndex(alertName);
            Alert inMemoryAlert = alertMap.get(alertName);
            if (indexedAlert == null) {
                //Alert has been deleted out from underneath us
                alertMap.remove(alertName);
                return false;
            } else if (inMemoryAlert == null) {
                logger.warn("Got claim attempt for alert [{}] that alert manager does not have but is in the index.", alertName);
                alertMap.put(alertName, indexedAlert); //This is an odd state to get into
            } else {
                if (!inMemoryAlert.isSameAlert(indexedAlert)) {
                    alertMap.put(alertName, indexedAlert); //Probably has been changed by another process and we missed the notification
                }
            }
            if (!indexedAlert.enabled()) {
                return false;
            }
            if (indexedAlert.running().equals(scheduleRunTime) || indexedAlert.running().isAfter(scheduleRunTime)) {
                //Someone else is already running this alert or this alert time has passed
                return false;
            }
        } catch (Throwable t) {
            throw new ElasticsearchException("Unable to load alert from index",t);
        }

        indexedAlert.running(scheduleRunTime);
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(ALERT_INDEX);
        updateRequest.type(ALERT_TYPE);
        updateRequest.id(alertName);
        updateRequest.version(indexedAlert.version());//Since we loaded this alert directly from the index the version should be correct
        XContentBuilder alertBuilder;
        try {
            alertBuilder = XContentFactory.jsonBuilder();
            indexedAlert.toXContent(alertBuilder, ToXContent.EMPTY_PARAMS);
        } catch (IOException ie) {
            throw new ElasticsearchException("Unable to serialize alert ["+ alertName + "]", ie);
        }
        updateRequest.doc(alertBuilder);
        updateRequest.retryOnConflict(0);

        try {
            client.update(updateRequest).actionGet();
        } catch (ElasticsearchException ee) {
            logger.error("Failed to update in claim", ee);
            return false;
        }

        Alert alert = alertMap.get(alertName);
        if (alert != null) {
            alert.running(scheduleRunTime);
        }
        return true;
    }

    private Alert getAlertFromIndex(String alertName) {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }

        GetRequest getRequest = Requests.getRequest(ALERT_INDEX);
        getRequest.type(ALERT_TYPE);
        getRequest.id(alertName);
        GetResponse getResponse = client.get(getRequest).actionGet();
        if (getResponse.isExists()) {
            return parseAlert(alertName, getResponse.getSourceAsMap(), getResponse.getVersion());
        } else {
            throw new ElasticsearchException("Unable to find [" + alertName + "] in the [" +ALERT_INDEX + "]" );
        }
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

    private void loadAlerts() {
        if (!client.admin().indices().prepareExists(ALERT_INDEX).execute().actionGet().isExists()) {
            createAlertsIndex();
        }

        SearchResponse searchResponse = client.prepareSearch().setSource(
                "{ \"query\" : " +
                        "{ \"match_all\" :  {}}," +
                        "\"size\" : \"100\"" +
                        "}"
        ).setTypes(ALERT_TYPE).setIndices(ALERT_INDEX).execute().actionGet();
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

    public long getLastEventCount(String alertName){
        return 0;
    }

    public boolean updateLastRan(String alertName, DateTime fireTime, DateTime scheduledTime) throws Exception {
        try {
            Alert alert = getAlertForName(alertName);
            alert.lastRan(fireTime);
            XContentBuilder alertBuilder = XContentFactory.jsonBuilder().prettyPrint();
            alert.toXContent(alertBuilder, ToXContent.EMPTY_PARAMS);
            logger.error(XContentHelper.convertToJson(alertBuilder.bytes(),false,true));
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.id(alertName);
            updateRequest.index(ALERT_INDEX);
            updateRequest.type(ALERT_TYPE);

            updateRequest.doc(alertBuilder);
            updateRequest.refresh(true);
            client.update(updateRequest).actionGet();
            return true;
        } catch (Throwable t) {
            logger.error("Failed to update alert [{}] with lastRan of [{}]",t, alertName, fireTime);
            return false;
        }
    }


    public boolean deleteAlert(String alertName) throws InterruptedException, ExecutionException {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }

        if (alertMap.remove(alertName) != null) {
            scheduler.deleteAlertFromSchedule(alertName);
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
        return false;
    }

    public boolean addAlert(String alertName, Alert alert, boolean persist) {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }

        if (!client.admin().indices().prepareExists(ALERT_INDEX).execute().actionGet().isExists()) {
            createAlertsIndex();
        }

        if (alertMap.putIfAbsent(alertName, alert) == null) {
            scheduler.addAlert(alertName, alert);
            if (persist) {
                return persistAlert(alertName, alert, IndexRequest.OpType.CREATE);
            } else {
                return true;
            }
        } else {
            throw new ElasticsearchIllegalArgumentException("There is already an alert named ["+alertName+"]");
        }
    }

    public boolean disableAlert(String alertName) {
        Alert alert = alertMap.get(alertName);
        if (alert == null) {
            throw new ElasticsearchIllegalArgumentException("Could not find an alert named [" + alertName + "]");
        }
        alert.enabled(false);
        return persistAlert(alertName, alert, IndexRequest.OpType.INDEX);
    }

    public boolean enableAlert(String alertName) {
        Alert alert = alertMap.get(alertName);
        if (alert == null) {
            throw new ElasticsearchIllegalArgumentException("Could not find an alert named [" + alertName + "]");
        }
        alert.enabled(true);
        return persistAlert(alertName, alert, IndexRequest.OpType.INDEX);
    }

    private boolean persistAlert(String alertName, Alert alert, IndexRequest.OpType opType) {
        XContentBuilder builder;
        try {
            builder = XContentFactory.jsonBuilder();
            alert.toXContent(builder, ToXContent.EMPTY_PARAMS);
            IndexRequest indexRequest = new IndexRequest(ALERT_INDEX, ALERT_TYPE, alertName);
            indexRequest.listenerThreaded(false);
            indexRequest.operationThreaded(false);
            indexRequest.refresh(true); //Always refresh after indexing an alert
            indexRequest.source(builder);
            indexRequest.opType(opType);
            IndexResponse indexResponse = client.index(indexRequest).actionGet();
            //@TODO : broadcast refresh here
            if (opType.equals(IndexRequest.OpType.CREATE)) {
                return indexResponse.isCreated();
            } else {
                return true;
            }
        } catch (IOException ie) {
            throw new ElasticsearchIllegalStateException("Unable to convert alert to JSON", ie);
        }
    }

    private Alert parseAlert(String alertId, SearchHit sh) {

        Map<String, Object> fields = sh.sourceAsMap();
        return parseAlert(alertId, fields, sh.getVersion());
    }



    public Alert parseAlert(String alertId, Map<String, Object> fields) {
        return parseAlert(alertId, fields, -1);
    }

    public Alert parseAlert(String alertId, Map<String, Object> fields, long version ) {

        logger.warn("Parsing : [{}]", alertId);
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
            actions = actionRegistry.parseActionsFromMap(actionMap);
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

    public Map<String,Alert> getSafeAlertMap() {
        return ImmutableMap.copyOf(alertMap);
    }

    public Alert getAlertForName(String alertName) {
        return alertMap.get(alertName);
    }

    public boolean isStarted() {
        return started.get();
    }

    public boolean addHistory(String alertName, boolean isTriggered, DateTime dateTime, DateTime scheduledTime,
                              SearchRequestBuilder srb, AlertTrigger trigger, long totalHits, List<AlertAction> actions,
                              List<String> indices) throws IOException{
        return actionManager.addHistory(alertName, isTriggered, dateTime, scheduledTime, srb, trigger, totalHits, actions, indices);
    }

    private final class AlertsClusterStateListener implements ClusterStateListener {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (!event.localNodeMaster()) { //We are not the master
                if (started.compareAndSet(false, true)) {
                    scheduler.clearAlerts();
                    alertMap.clear();
                }

                if (startActions.compareAndSet(false, true)) {
                    //If actionManager was running and we aren't the master stop
                    actionManager.doStop(); //Safe to call this multiple times, it's a noop if we are already stopped
                }
                return;
            }

            if (!started.get()) {
                IndexMetaData alertIndexMetaData = event.state().getMetaData().index(ALERT_INDEX);
                if (alertIndexMetaData != null) {
                    if (event.state().routingTable().index(ALERT_INDEX).allPrimaryShardsActive()) {
                        started.set(true);
                        // TODO: the starter flag should only be set to true once the alert loader has completed.
                        // Right now there is a window of time between when started=true and loading has completed where
                        // alerts can get lost
                        threadPool.executor(ThreadPool.Names.GENERIC).execute(new AlertLoader());
                    }
                }
            }

            if (!startActions.get()) {
                IndexMetaData indexMetaData = event.state().getMetaData().index(AlertActionManager.ALERT_HISTORY_INDEX);
                if (indexMetaData != null) {
                    if (event.state().routingTable().index(ALERT_INDEX).allPrimaryShardsActive()) {
                        startActions.set(true);
                        actionManager.doStart();
                    }
                }
            }
        }

    }

    private class AlertLoader implements Runnable {
        @Override
        public void run() {
            // TODO: have some kind of retry mechanism?
            try {
                loadAlerts();
                sendAlertsToScheduler();
            } catch (Exception e) {
                logger.warn("Error during loading of alerts from an existing .alerts index... refresh the alerts manually");
            }
            started.set(true);

        }
    }

}
