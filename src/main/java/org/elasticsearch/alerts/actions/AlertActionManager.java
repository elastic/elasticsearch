/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.alerts.AlertManager;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.alerts.triggers.TriggerManager;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class AlertActionManager {

    public static final String ALERT_NAME_FIELD = "alertName";
    public static final String TRIGGERED_FIELD = "triggered";
    public static final String FIRE_TIME_FIELD = "fireTime";
    public static final String SCHEDULED_FIRE_TIME_FIELD = "scheduledFireTime";
    public static final String TRIGGER_FIELD = "trigger";
    public static final String QUERY_RAN_FIELD = "queryRan";
    public static final String NUMBER_OF_RESULTS_FIELD = "numberOfResults";
    public static final String ACTIONS_FIELD = "actions";
    public static final String INDICES_FIELD = "indices";
    public static final String ALERT_HISTORY_INDEX = "alerthistory";
    public static final String ALERT_HISTORY_TYPE = "alerthistory";

    private final Client client;
    private final AlertManager alertManager;
    private final AlertActionRegistry actionRegistry;
    private final ThreadPool threadPool;

    private final ESLogger logger = Loggers.getLogger(AlertActionManager.class);

    private BlockingQueue<AlertActionEntry> jobsToBeProcessed = new LinkedBlockingQueue<>();

    public final AtomicBoolean running = new AtomicBoolean(false);
    private Executor readerExecutor;

    private static AlertActionEntry END_ENTRY = new AlertActionEntry();

    class AlertHistoryRunnable implements Runnable {
        AlertActionEntry entry;

        AlertHistoryRunnable(AlertActionEntry entry) {
            this.entry = entry;
        }

        @Override
        public void run() {
            try {
                if (claimAlertHistoryEntry(entry)) {
                    alertManager.doAction(alertManager.getAlertForName(entry.getAlertName()), entry, entry.getScheduledTime());
                    updateHistoryEntry(entry, AlertActionState.ACTION_PERFORMED);
                } else {
                    logger.warn("Unable to claim alert history entry" + entry);
                }
            } catch (Throwable t) {
                logger.error("Failed to execute alert action", t);
            }


        }
    }

    class QueueLoaderThread implements Runnable {
        @Override
        public void run() {
            boolean success = false;
            do {
                try {
                    success = loadQueue();
                } catch (Exception e) {
                    logger.error("Unable to load the job queue", e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {

                    }
                }
            } while (!success);
        }
    }

    class QueueReaderThread implements Runnable {
        @Override
        public void run() {
            try {
                logger.debug("Starting thread to read from the job queue");
                while (running.get()) {
                    AlertActionEntry entry = null;
                    do {
                        try {
                            entry = jobsToBeProcessed.take();
                        } catch (InterruptedException ie) {
                            if (!running.get()) {
                                break;
                            }
                        }
                    } while (entry == null);

                    if (!running.get() || entry == END_ENTRY) {
                        logger.debug("Stopping thread to read from the job queue");
                    }

                    threadPool.executor(ThreadPool.Names.MANAGEMENT)
                            .execute(new AlertHistoryRunnable(entry));
                }
            } catch (Throwable t) {
                logger.error("Error during reader thread", t);
            }
        }
    }

    public AlertActionManager(Client client, AlertManager alertManager,
                              AlertActionRegistry actionRegistry,
                              ThreadPool threadPool) {
        this.client = client;
        this.alertManager = alertManager;
        this.actionRegistry = actionRegistry;
        this.threadPool = threadPool;
    }

    public void doStart() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting job queue");
            readerExecutor = threadPool.executor(ThreadPool.Names.GENERIC);
            readerExecutor.execute(new QueueReaderThread());
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new QueueLoaderThread());
        }
    }

    public void doStop() {
        stopIfRunning();
    }

    public boolean loadQueue() {
        if (!client.admin().indices().prepareExists(ALERT_HISTORY_INDEX).execute().actionGet().isExists()) {
            createAlertHistoryIndex();
        }

        //@TODO: change to scan/scroll if we get back over 100
        SearchResponse searchResponse = client.prepareSearch().setSource(
                "{ \"query\" : " +
                        "{ \"term\" :  {" +
                        "\"" + AlertActionState.FIELD_NAME + "\" : \"" + AlertActionState.ACTION_NEEDED.toString() + "\"}}," +
                        "\"size\" : \"100\"" +
                        "}"
        ).setTypes(ALERT_HISTORY_TYPE).setIndices(ALERT_HISTORY_INDEX).setListenerThreaded(false).execute().actionGet();

        for (SearchHit sh : searchResponse.getHits()) {
            String historyId = sh.getId();
            AlertActionEntry historyEntry = parseHistory(historyId, sh, sh.version());
            assert historyEntry.getEntryState() == AlertActionState.ACTION_NEEDED;
            jobsToBeProcessed.add(historyEntry);
        }

        return true;
    }



    protected AlertActionEntry parseHistory(String historyId, SearchHit sh, long version) {
        Map<String, Object> fields = sh.sourceAsMap();
        return parseHistory(historyId, fields, version);
    }

    protected AlertActionEntry parseHistory(String historyId, Map<String,Object> fields, long version) {
        return parseHistory(historyId, fields, version, actionRegistry, logger);
    }

    protected static AlertActionEntry parseHistory(String historyId, Map<String,Object> fields, long version,
                                                   AlertActionRegistry actionRegistry, ESLogger logger) {
        String alertName = fields.get(ALERT_NAME_FIELD).toString();
        boolean triggered = (Boolean)fields.get(TRIGGERED_FIELD);
        DateTime fireTime = new DateTime(fields.get(FIRE_TIME_FIELD).toString());
        DateTime scheduledFireTime = new DateTime(fields.get(SCHEDULED_FIRE_TIME_FIELD).toString());
        AlertTrigger trigger = TriggerManager.parseTriggerFromMap((Map<String,Object>)fields.get(TRIGGER_FIELD));
        String queryRan = fields.get(QUERY_RAN_FIELD).toString();
        long numberOfResults = ((Number)fields.get(NUMBER_OF_RESULTS_FIELD)).longValue();
        Object actionObj = fields.get(ACTIONS_FIELD);
        List<AlertAction> actions;
        if (actionObj instanceof Map) {
            Map<String, Object> actionMap = (Map<String, Object>) actionObj;
            actions = actionRegistry.parseActionsFromMap(actionMap);
        } else {
            throw new ElasticsearchException("Unable to parse actions [" + actionObj + "]");
        }

        List<String> indices = new ArrayList<>();
        if (fields.get(INDICES_FIELD) != null && fields.get(INDICES_FIELD) instanceof List){
            indices = (List<String>)fields.get(INDICES_FIELD);
        } else {
            logger.debug("Indices : " + fields.get(INDICES_FIELD) + " class " +
                    (fields.get(INDICES_FIELD) != null ? fields.get(INDICES_FIELD).getClass() : null ));
        }

        String stateString = fields.get(AlertActionState.FIELD_NAME).toString();
        AlertActionState state = AlertActionState.fromString(stateString);

        return new AlertActionEntry(historyId, version, alertName, triggered, fireTime, scheduledFireTime, trigger, queryRan,
                numberOfResults, actions, indices, state);
    }


    public boolean addHistory(String alertName, boolean triggered,
                              DateTime fireTime, DateTime scheduledFireTime, SearchRequestBuilder triggeringQuery,
                              AlertTrigger trigger, long numberOfResults,
                              List<AlertAction> actions,
                              @Nullable List<String> indices) throws IOException {

        if (!client.admin().indices().prepareExists(ALERT_HISTORY_INDEX).execute().actionGet().isExists()) {
            ClusterHealthStatus chs = createAlertHistoryIndex();
        }

        AlertActionState state = AlertActionState.NO_ACTION_NEEDED;
        if (triggered && !actions.isEmpty()) {
            state = AlertActionState.ACTION_NEEDED;
        }

        AlertActionEntry entry = new AlertActionEntry(alertName + " " + scheduledFireTime.toDateTimeISO(), 1, alertName, triggered, fireTime, scheduledFireTime, trigger,
                triggeringQuery.toString(), numberOfResults, actions, indices, state);

        XContentBuilder historyEntry = XContentFactory.jsonBuilder();
        entry.toXContent(historyEntry, ToXContent.EMPTY_PARAMS);

        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(ALERT_HISTORY_INDEX);
        indexRequest.type(ALERT_HISTORY_TYPE);
        indexRequest.id(entry.getId());
        indexRequest.source(historyEntry);
        indexRequest.listenerThreaded(false);
        indexRequest.operationThreaded(false);
        indexRequest.refresh(true); //Always refresh after indexing an alert
        indexRequest.opType(IndexRequest.OpType.CREATE);
        try {
            if (client.index(indexRequest).actionGet().isCreated()) {
                jobsToBeProcessed.add(entry);
                return true;
            } else {
                return false;
            }
        } catch (DocumentAlreadyExistsException daee){
            logger.warn("Someone has already created a history entry for this alert run");
            return false;
        }
    }

    private void stopIfRunning() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping job queue");
            jobsToBeProcessed.add(END_ENTRY);
        }
    }


    private ClusterHealthStatus createAlertHistoryIndex() {
        CreateIndexResponse cir = client.admin().indices().prepareCreate(ALERT_HISTORY_INDEX).addMapping(ALERT_HISTORY_TYPE).execute().actionGet(); //TODO FIX MAPPINGS
        if (!cir.isAcknowledged()) {
            logger.error("Create [{}] was not acknowledged", ALERT_HISTORY_INDEX);
        }
        ClusterHealthResponse actionGet = client.admin().cluster()
                .health(Requests.clusterHealthRequest(ALERT_HISTORY_INDEX).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();

        return actionGet.getStatus();
    }



    private AlertActionEntry getHistoryEntryFromIndex(String entryId) {
        GetRequest getRequest = Requests.getRequest(ALERT_HISTORY_INDEX);
        getRequest.type(ALERT_HISTORY_TYPE);
        getRequest.id(entryId);
        GetResponse getResponse = client.get(getRequest).actionGet();
        if (getResponse.isExists()) {
            return parseHistory(entryId, getResponse.getSourceAsMap(), getResponse.getVersion());
        } else {
            throw new ElasticsearchException("Unable to find [" + entryId + "] in the [" + ALERT_HISTORY_INDEX + "]" );
        }
    }

    private void updateHistoryEntry(AlertActionEntry entry, AlertActionState actionPerformed) {
        entry.setEntryState(AlertActionState.ACTION_PERFORMED);

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(ALERT_HISTORY_INDEX);
        updateRequest.type(ALERT_HISTORY_TYPE);
        updateRequest.id(entry.getId());

        entry.setEntryState(actionPerformed);
        XContentBuilder historyBuilder;
        try {
            historyBuilder = XContentFactory.jsonBuilder();
            entry.toXContent(historyBuilder, ToXContent.EMPTY_PARAMS);
        } catch (IOException ie) {
            throw new ElasticsearchException("Unable to serialize alert history entry ["+ entry.getId() + "]", ie);
        }
        updateRequest.doc(historyBuilder);

        try {
            client.update(updateRequest).actionGet();
        } catch (ElasticsearchException ee) {
            logger.error("Failed to update in claim", ee);
        }
    }

    private boolean claimAlertHistoryEntry(AlertActionEntry entry) {
        AlertActionEntry indexedHistoryEntry;
        try {
            indexedHistoryEntry = getHistoryEntryFromIndex(entry.getId());
            if (indexedHistoryEntry.getEntryState() != AlertActionState.ACTION_NEEDED) {
                //Someone else is doing or has done this action
                return false;
            }
            entry.setEntryState(AlertActionState.ACTION_UNDERWAY);

            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(ALERT_HISTORY_INDEX);
            updateRequest.type(ALERT_HISTORY_TYPE);
            updateRequest.id(entry.getId());
            updateRequest.version(entry.getVersion());//Since we loaded this alert directly from the index the version should be correct

            XContentBuilder historyBuilder;
            try {
                historyBuilder = XContentFactory.jsonBuilder();
                entry.toXContent(historyBuilder, ToXContent.EMPTY_PARAMS);
            } catch (IOException ie) {
                throw new ElasticsearchException("Unable to serialize alert history entry ["+ entry.getId() + "]", ie);
            }
            updateRequest.doc(historyBuilder);
            updateRequest.retryOnConflict(0);

            try {
                client.update(updateRequest).actionGet();
            } catch (ElasticsearchException ee) {
                logger.error("Failed to update in claim", ee);
                return false;
            }

        } catch (Throwable t) {
            logger.error("Failed to claim history entry " + entry, t);
            return false;
        }
        return true;
    }



}
