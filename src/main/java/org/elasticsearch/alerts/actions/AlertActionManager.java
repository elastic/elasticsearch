/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.LoadingListener;
import org.elasticsearch.alerts.triggers.TriggerManager;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class AlertActionManager extends AbstractComponent {

    public static final String ALERT_NAME_FIELD = "alert_name";
    public static final String TRIGGERED_FIELD = "triggered";
    public static final String FIRE_TIME_FIELD = "fire_time";
    public static final String SCHEDULED_FIRE_TIME_FIELD = "scheduled_fire_time";
    public static final String TRIGGER_FIELD = "trigger";
    public static final String REQUEST = "request_binary";
    public static final String RESPONSE = "response_binary";
    public static final String ACTIONS_FIELD = "actions";

    public static final String ALERT_HISTORY_INDEX = "alerthistory";
    public static final String ALERT_HISTORY_TYPE = "alerthistory";

    private final Client client;
    private final ThreadPool threadPool;
    private final AlertsStore alertsStore;
    private final AlertActionRegistry actionRegistry;

    private final BlockingQueue<AlertActionEntry> jobsToBeProcessed = new LinkedBlockingQueue<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);

    private static AlertActionEntry END_ENTRY = new AlertActionEntry();

    private class AlertHistoryRunnable implements Runnable {

        private final AlertActionEntry entry;

        private AlertHistoryRunnable(AlertActionEntry entry) {
            this.entry = entry;
        }

        @Override
        public void run() {
            try {
                if (claimAlertHistoryEntry(entry)) {
                    Alert alert = alertsStore.getAlert(entry.getAlertName());
                    DateTime lastActionFire = alert.lastActionFire();
                    DateTime scheduledTime = entry.getScheduledTime();
                    if (lastActionFire != null) {
                        long msSinceLastAction = scheduledTime.getMillis() - lastActionFire.getMillis();
                        logger.trace("last action fire [{}]", lastActionFire);
                        logger.trace("msSinceLastAction [{}]", msSinceLastAction);
                    } else {
                        logger.trace("This is the first time this alert has run");
                    }

                    actionRegistry.doAction(alert, entry);
                    logger.debug("Did action !");

                    alert.lastActionFire(scheduledTime);
                    alertsStore.updateAlert(alert);
                    updateHistoryEntry(entry, AlertActionState.ACTION_PERFORMED);
                } else {
                    logger.warn("Unable to claim alert history entry" + entry);
                }
            } catch (Throwable t) {
                logger.error("Failed to execute alert action", t);
            }
        }
    }

    private class QueueReaderThread implements Runnable {

        @Override
        public void run() {
            try {
                logger.debug("Starting thread to read from the job queue");
                while (started()) {
                    AlertActionEntry entry = null;
                    do {
                        try {
                            entry = jobsToBeProcessed.take();
                        } catch (InterruptedException ie) {
                            if (!started()) {
                                break;
                            }
                        }
                    } while (entry == null);

                    if (!started() || entry == END_ENTRY) {
                        logger.debug("Stopping thread to read from the job queue");
                        return;
                    }
                    threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new AlertHistoryRunnable(entry));
                }
            } catch (Throwable t) {
                logger.error("Error during reader thread", t);
            }
        }
    }

    @Inject
    public AlertActionManager(Settings settings, Client client, AlertActionRegistry actionRegistry, ThreadPool threadPool, AlertsStore alertsStore) {
        super(settings);
        this.client = client;
        this.actionRegistry = actionRegistry;
        this.threadPool = threadPool;
        this.alertsStore = alertsStore;
    }

    public void start(ClusterState state, final LoadingListener listener) {
        IndexMetaData indexMetaData = state.getMetaData().index(ALERT_HISTORY_INDEX);
        if (indexMetaData != null) {
            if (state.routingTable().index(ALERT_HISTORY_INDEX).allPrimaryShardsActive()) {
                if (this.state.compareAndSet(State.STOPPED, State.LOADING)) {
                    threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                        @Override
                        public void run() {
                            boolean success = false;
                            try {
                                success = loadQueue();
                            } catch (Exception e) {
                                logger.error("Unable to load unfinished jobs into the job queue", e);
                            } finally {
                                if (success) {
                                    if (AlertActionManager.this.state.compareAndSet(State.LOADING, State.STARTED)) {
                                        doStart();
                                        listener.onSuccess();
                                    }
                                } else {
                                    if (AlertActionManager.this.state.compareAndSet(State.LOADING, State.STOPPED)) {
                                        listener.onFailure();
                                    }
                                }
                            }
                        }
                    });
                }
            }
        } else {
            if (this.state.compareAndSet(State.STOPPED, State.STARTED)) {
                doStart();
                listener.onSuccess();
            }
        }
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPED)) {
            logger.info("Stopping job queue");
            jobsToBeProcessed.add(END_ENTRY);
        }
    }

    public boolean started() {
        return state.get() == State.STARTED;
    }

    private void doStart() {
        logger.info("Starting job queue");
        threadPool.executor(ThreadPool.Names.GENERIC).execute(new QueueReaderThread());
    }

    public boolean loadQueue() {
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
        return parseHistory(historyId, sh.getSourceRef(), version);
    }

    protected AlertActionEntry parseHistory(String historyId, BytesReference source, long version) {
        return parseHistory(historyId, source, version, actionRegistry);
    }

    protected static AlertActionEntry parseHistory(String historyId, BytesReference source, long version,
                                                   AlertActionRegistry actionRegistry) {
        AlertActionEntry entry = new AlertActionEntry();
        entry.setId(historyId);
        entry.setVersion(version);
        try (XContentParser parser = XContentHelper.createParser(source)) {
            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            assert token == XContentParser.Token.START_OBJECT;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    switch (currentFieldName) {
                        case ACTIONS_FIELD:
                            entry.setActions(actionRegistry.instantiateAlertActions(parser));
                            break;
                        case TRIGGER_FIELD:
                            entry.setTrigger(TriggerManager.parseTrigger(parser));
                            break;
                        case "response":
                            // Ignore this, the binary form is already read
                            parser.skipChildren();
                            break;
                        default:
                            throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    switch (currentFieldName) {
                        case ALERT_NAME_FIELD:
                            entry.setAlertName(parser.text());
                            break;
                        case TRIGGERED_FIELD:
                            entry.setTriggered(parser.booleanValue());
                            break;
                        case FIRE_TIME_FIELD:
                            entry.setFireTime(DateTime.parse(parser.text()));
                            break;
                        case SCHEDULED_FIRE_TIME_FIELD:
                            entry.setScheduledTime(DateTime.parse(parser.text()));
                            break;
                        case REQUEST:
                            SearchRequest request = new SearchRequest();
                            request.readFrom(new BytesStreamInput(parser.binaryValue(), false));
                            entry.setSearchRequest(request);
                            break;
                        case RESPONSE:
                            SearchResponse response = new SearchResponse();
                            response.readFrom(new BytesStreamInput(parser.binaryValue(), false));
                            entry.setSearchResponse(response);
                            break;
                        case AlertActionState.FIELD_NAME:
                            entry.setEntryState(AlertActionState.fromString(parser.text()));
                            break;
                        default:
                            throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "]");
                }
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Error during parsing alert action", e);
        }
        return entry;
    }

    public void addAlertAction(Alert alert, TriggerResult result, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
        AlertActionState state = AlertActionState.NO_ACTION_NEEDED;
        if (result.isTriggered() && !alert.actions().isEmpty()) {
            state = AlertActionState.ACTION_NEEDED;
        }

        AlertActionEntry entry = new AlertActionEntry(alert, result, scheduledFireTime, fireTime, state);
        XContentBuilder historyEntry = XContentFactory.jsonBuilder();
        entry.toXContent(historyEntry, ToXContent.EMPTY_PARAMS);

        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(ALERT_HISTORY_INDEX);
        indexRequest.type(ALERT_HISTORY_TYPE);
        indexRequest.id(entry.getId());
        indexRequest.source(historyEntry);
        indexRequest.listenerThreaded(false);
        indexRequest.opType(IndexRequest.OpType.CREATE);
        client.index(indexRequest).actionGet();
        if (state != AlertActionState.NO_ACTION_NEEDED) {
            jobsToBeProcessed.add(entry);
        }
    }

    private AlertActionEntry getHistoryEntryFromIndex(String entryId) {
        GetRequest getRequest = Requests.getRequest(ALERT_HISTORY_INDEX);
        getRequest.type(ALERT_HISTORY_TYPE);
        getRequest.id(entryId);
        GetResponse getResponse = client.get(getRequest).actionGet();
        if (getResponse.isExists()) {
            return parseHistory(entryId, getResponse.getSourceAsBytesRef(), getResponse.getVersion());
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

    private enum State {

        STOPPED,
        LOADING,
        STARTED

    }

}
