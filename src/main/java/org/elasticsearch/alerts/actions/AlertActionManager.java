/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.alerts.*;
import org.elasticsearch.alerts.plugin.AlertsPlugin;
import org.elasticsearch.alerts.triggers.TriggerManager;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class AlertActionManager extends AbstractComponent {

    public static final String ALERT_NAME_FIELD = "alert_name";
    public static final String TRIGGERED_FIELD = "triggered";
    public static final String FIRE_TIME_FIELD = "fire_time";
    public static final String SCHEDULED_FIRE_TIME_FIELD = "scheduled_fire_time";
    public static final String ERROR_MESSAGE = "error_msg";
    public static final String TRIGGER_FIELD = "trigger";
    public static final String REQUEST = "request";
    public static final String RESPONSE = "response";
    public static final String ACTIONS_FIELD = "actions";
    public static final String STATE = "state";

    public static final String ALERT_HISTORY_INDEX_PREFIX = ".alert_history_";
    public static final DateTimeFormatter alertHistoryIndexTimeFormat = DateTimeFormat.forPattern("YYYY-MM-dd");
    public static final String ALERT_HISTORY_TYPE = "alerthistory";

    private static AlertActionEntry END_ENTRY = new AlertActionEntry();

    private final Client client;
    private AlertManager alertManager;
    private final ThreadPool threadPool;
    private final AlertsStore alertsStore;
    private final TriggerManager triggerManager;
    private final TemplateHelper templateHelper;
    private final AlertActionRegistry actionRegistry;

    private final int scrollSize;
    private final TimeValue scrollTimeout;

    private final AtomicLong largestQueueSize = new AtomicLong(0);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final BlockingQueue<AlertActionEntry> actionsToBeProcessed = new LinkedBlockingQueue<>();

    @Inject
    public AlertActionManager(Settings settings, Client client, AlertActionRegistry actionRegistry,
                              ThreadPool threadPool, AlertsStore alertsStore, TriggerManager triggerManager,
                              TemplateHelper templateHelper) {
        super(settings);
        this.client = client;
        this.actionRegistry = actionRegistry;
        this.threadPool = threadPool;
        this.alertsStore = alertsStore;
        this.triggerManager = triggerManager;
        this.templateHelper = templateHelper;
        // Not using component settings, to let AlertsStore and AlertActionManager share the same settings
        this.scrollSize = settings.getAsInt("alerts.scroll.size", 100);
        this.scrollTimeout = settings.getAsTime("alerts.scroll.timeout", TimeValue.timeValueSeconds(30));
    }

    public void setAlertManager(AlertManager alertManager){
        this.alertManager = alertManager;
    }

    public boolean start(ClusterState state) {
        if (started.get()) {
            return true;
        }
        String[] indices = state.metaData().concreteIndices(IndicesOptions.lenientExpandOpen(), ALERT_HISTORY_INDEX_PREFIX + "*");
        if (indices.length == 0) {
            logger.info("No previous .alerthistory index, skip loading of alert actions");
            templateHelper.checkAndUploadIndexTemplate(state, "alerthistory");
            doStart();
            return true;
        }
        for (String index : indices) {
            IndexMetaData indexMetaData = state.getMetaData().index(index);
            if (indexMetaData != null) {
                if (!state.routingTable().index(index).allPrimaryShardsActive()) {
                    logger.info("Not all primary shards of the [{}] index are started", index);
                    return false;
                }
            }
        }

        try {
            loadQueue();
        } catch (Exception e) {
            logger.error("Unable to load unfinished jobs into the job queue", e);
            actionsToBeProcessed.clear();
        }
        templateHelper.checkAndUploadIndexTemplate(state, "alerthistory");
        doStart();
        return true;
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            actionsToBeProcessed.clear();
            actionsToBeProcessed.add(END_ENTRY);
            logger.info("Stopped job queue");
        }
    }

    public boolean started() {
        return started.get();
    }

    /**
     * Calculates the correct alert history index name for a given time using alertHistoryIndexTimeFormat
     */
    public static String getAlertHistoryIndexNameForTime(DateTime time) {
        StringBuffer sb = new StringBuffer(ALERT_HISTORY_INDEX_PREFIX);
        alertHistoryIndexTimeFormat.printTo(sb, time);
        return sb.toString();
    }

    private void doStart() {
        logger.info("Starting job queue");
        if (started.compareAndSet(false, true)) {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(new QueueReaderThread());
        }
    }

    public void loadQueue() {
        client.admin().indices().refresh(new RefreshRequest(ALERT_HISTORY_INDEX_PREFIX + "*")).actionGet();

        SearchResponse response = client.prepareSearch(ALERT_HISTORY_INDEX_PREFIX + "*")
                .setQuery(QueryBuilders.termQuery(STATE, AlertActionState.SEARCH_NEEDED.toString()))
                .setSearchType(SearchType.SCAN)
                .setScroll(scrollTimeout)
                .setSize(scrollSize)
                .setTypes(ALERT_HISTORY_TYPE)
                .get();
        try {
            if (response.getHits().getTotalHits() > 0) {
                response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeout).get();
                while (response.getHits().hits().length != 0) {
                    for (SearchHit sh : response.getHits()) {
                        String historyId = sh.getId();
                        AlertActionEntry historyEntry = parseHistory(historyId, sh.getSourceRef(), sh.version(), actionRegistry);
                        assert historyEntry.getState() == AlertActionState.SEARCH_NEEDED;
                        actionsToBeProcessed.add(historyEntry);
                    }
                    response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollTimeout).get();
                }
            }
        } finally {
            client.prepareClearScroll().addScrollId(response.getScrollId()).get();
        }
        logger.info("Loaded [{}] actions from the alert history index into actions queue", actionsToBeProcessed.size());
        largestQueueSize.set(actionsToBeProcessed.size());
    }

    AlertActionEntry parseHistory(String historyId, BytesReference source, long version, AlertActionRegistry actionRegistry) {
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
                            entry.setTrigger(triggerManager.instantiateAlertTrigger(parser));
                            break;
                        case REQUEST:
                            entry.setSearchRequest(AlertUtils.readSearchRequest(parser));
                            break;
                        case RESPONSE:
                            entry.setSearchResponse(parser.map());
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
                        case ERROR_MESSAGE:
                            entry.setErrorMsg(parser.textOrNull());
                            break;
                        case STATE:
                            entry.setState(AlertActionState.fromString(parser.text()));
                            break;
                        default:
                            throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    if (token == XContentParser.Token.VALUE_NULL) {
                        logger.warn("Got null value for [{}]", currentFieldName);
                    } else {
                        throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "] for [" + currentFieldName + "]");
                    }
                }
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Error during parsing alert action", e);
        }
        return entry;
    }

    public void addAlertAction(Alert alert, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
        ensureStarted();
        logger.debug("Adding alert action for alert [{}]", alert.alertName());
        String alertHistoryIndex = getAlertHistoryIndexNameForTime(scheduledFireTime);
        AlertActionEntry entry = new AlertActionEntry(alert, scheduledFireTime, fireTime, AlertActionState.SEARCH_NEEDED);
        IndexResponse response = client.prepareIndex(alertHistoryIndex, ALERT_HISTORY_TYPE, entry.getId())
                .setSource(XContentFactory.jsonBuilder().value(entry))
                .setOpType(IndexRequest.OpType.CREATE)
                .get();
        entry.setVersion(response.getVersion());

        long currentSize = actionsToBeProcessed.size() + 1;
        actionsToBeProcessed.add(entry);
        long currentLargestQueueSize = largestQueueSize.get();
        boolean done = false;
        while (!done) {
            if (currentSize > currentLargestQueueSize) {
                done = largestQueueSize.compareAndSet(currentLargestQueueSize, currentSize);
            } else {
                break;
            }
            currentLargestQueueSize = largestQueueSize.get();
        }
    }


    private void updateHistoryEntry(AlertActionEntry entry) throws IOException {
        ensureStarted();
        IndexResponse response = client.prepareIndex(getAlertHistoryIndexNameForTime(entry.getScheduledTime()), ALERT_HISTORY_TYPE, entry.getId())
                .setSource(XContentFactory.jsonBuilder().value(entry))
                .get();
        entry.setVersion(response.getVersion());
    }


    private void updateHistoryEntry(AlertActionEntry entry, AlertActionState actionPerformed) throws IOException {
        entry.setState(actionPerformed);
        updateHistoryEntry(entry);
    }

    public long getQueueSize() {
        return actionsToBeProcessed.size();
    }

    public long getLargestQueueSize() {
        return largestQueueSize.get();
    }

    private void ensureStarted() {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("not started");
        }
    }

    private class AlertHistoryRunnable implements Runnable {

        private final AlertActionEntry entry;

        private AlertHistoryRunnable(AlertActionEntry entry) {
            this.entry = entry;
        }

        @Override
        public void run() {
            try {
                Alert alert = alertsStore.getAlert(entry.getAlertName());
                if (alert == null) {
                    entry.setErrorMsg("Alert was not found in the alerts store");
                    updateHistoryEntry(entry, AlertActionState.ERROR);
                    return;
                }
                updateHistoryEntry(entry, AlertActionState.SEARCH_UNDERWAY);
                logger.debug("Running an alert action entry for [{}]", entry.getAlertName());
                TriggerResult trigger = alertManager.executeAlert(entry);
                if (trigger.isTriggered()) {
                    if (entry.getState() != AlertActionState.THROTTLED) {
                        entry.setState(AlertActionState.ACTION_PERFORMED);
                    }
                } else {
                    entry.setState(AlertActionState.NO_ACTION_NEEDED);
                }
                updateHistoryEntry(entry);
            } catch (Exception e) {
                if (started()) {
                    logger.warn("Failed to execute alert action", e);
                    try {
                        entry.setErrorMsg(e.getMessage());
                        updateHistoryEntry(entry, AlertActionState.ERROR);
                    } catch (Exception e2) {
                        logger.error("Failed to update action history entry with the error message", e2);
                    }
                } else {
                    logger.debug("Failed to execute alert action after shutdown", e);
                }
            }
        }
    }

    private class QueueReaderThread implements Runnable {

        @Override
        public void run() {
            try {
                logger.debug("Starting thread to read from the job queue");
                while (started()) {
                    AlertActionEntry entry = actionsToBeProcessed.take();
                    if (!started() || entry == END_ENTRY) {
                        logger.info("Stopping thread to read from the job queue");
                        return;
                    }
                    threadPool.executor(AlertsPlugin.ALERT_THREAD_POOL_NAME).execute(new AlertHistoryRunnable(entry));
                }
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                } else {
                    if (started()) {
                        logger.error("Error during reader thread, restarting queue reader thread...", e);
                        threadPool.executor(ThreadPool.Names.GENERIC).execute(new QueueReaderThread());
                    } else {
                        logger.error("Error during reader thread", e);
                    }
                }
            }
        }
    }

}
