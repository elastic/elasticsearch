/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.ingest.geoip.GeoIpTaskState.Metadata;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class GeoIpDownloader extends PersistentTasksExecutor<PersistentTaskParams> implements ClusterStateListener {

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting("geoip.downloader.poll.interval",
        TimeValue.timeValueDays(3), TimeValue.timeValueDays(1), Property.Dynamic, Property.NodeScope);
    public static final Setting<String> ENDPOINT_SETTING = Setting.simpleString("geoip.downloader.endpoint",
        "https://paisano.elastic.dev/v1/geoip/database", Property.NodeScope);
    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting("geoip.downloader.enabled", true, Property.Dynamic,
        Property.NodeScope);

    public static final String GEOIP_DOWNLOADER = "geoip-downloader";
    static final String DATABASES_INDEX = ".geoip_databases";
    static final int MAX_CHUNK_SIZE = 1024 * 1024;

    private final Client client;
    private final HttpClient httpClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Logger logger = LogManager.getLogger(GeoIpDownloader.class);
    private final PersistentTasksService persistentTasksService;

    //visible for testing
    protected volatile GeoIpTaskState state;
    private volatile TimeValue pollInterval;
    private volatile String endpoint;
    private volatile AllocatedPersistentTask persistentTask;
    private volatile Scheduler.ScheduledCancellable scheduled;
    private volatile boolean enabled;

    GeoIpDownloader(Client client, HttpClient httpClient, ClusterService clusterService, ThreadPool threadPool, Settings settings) {
        super(GEOIP_DOWNLOADER, ThreadPool.Names.GENERIC);
        this.client = new OriginSettingClient(client, "geoip");
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);
        endpoint = ENDPOINT_SETTING.get(settings);
        pollInterval = POLL_INTERVAL_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::setPollInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENDPOINT_SETTING, this::setEndpoint);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
        enabled = ENABLED_SETTING.get(settings);
        if (enabled) {
            clusterService.addListener(this);
        }
    }

    public void setPollInterval(TimeValue pollInterval) {
        this.pollInterval = pollInterval;
        if (scheduled != null && scheduled.cancel()) {
            scheduleNextRun(new TimeValue(1));
        }
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            startPersistentTask();
        } else {
            if (scheduled != null) {
                scheduled.cancel();
            }
            persistentTasksService.sendRemoveRequest(GEOIP_DOWNLOADER, ActionListener.wrap(r -> {
                state = ((GeoIpTaskState) r.getState()).copy();
            }, e -> logger.error("could not remove task [" + GEOIP_DOWNLOADER + "]", e)));
        }
    }

    //visible for testing
    void updateDatabases() throws IOException {
        logger.info("updating geoip databases");
        List<Map<String, Object>> response = fetchDatabasesOverview();
        for (Map<String, Object> res : response) {
            processDatabase(res);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> fetchDatabasesOverview() throws IOException {
        byte[] data = httpClient.getBytes(endpoint + "?key=11111111-1111-1111-1111-111111111111");
        try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, data)) {
            return (List<T>) parser.list();
        }
    }

    //visible for testing
    void processDatabase(Map<String, Object> databaseInfo) {
        Map<String, Metadata> currentDatabases = state.getDatabases();
        String name = databaseInfo.get("name").toString().replace(".gz", "");
        boolean databaseInState = currentDatabases.containsKey(name);
        String md5 = (String) databaseInfo.get("md5_hash");
        if (databaseInState && Objects.equals(md5, currentDatabases.get(name).getMd5())) {
            updateTimestamp(currentDatabases, name);
            return;
        }
        logger.info("updating geoip database [" + name + "]");
        String url = databaseInfo.get("url").toString();
        try (InputStream is = httpClient.get(url)) {
            int firstChunk = databaseInState ? currentDatabases.get(name).getLastChunk() + 1 : 0;
            int lastChunk = indexChunks(name, is, firstChunk);
            if (lastChunk > firstChunk) {
                currentDatabases.put(name, new Metadata(System.currentTimeMillis(), firstChunk, lastChunk - 1, md5));
                if (updateTaskState(name)) {
                    logger.info("updated geoip database [" + name + "]");
                    deleteOldChunks(name, firstChunk);
                }
            }
        } catch (Exception e) {
            logger.error("error updating geoip database [" + name + "]", e);
        }
    }

    //visible for testing
    void deleteOldChunks(String name, int firstChunk) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
            .filter(new MatchQueryBuilder("name", name))
            .filter(new RangeQueryBuilder("chunk").to(firstChunk, false));
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        request.indices(DATABASES_INDEX);
        request.setQuery(queryBuilder);
        client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(r -> {
        }, e -> logger.warn("could not delete old chunks for geoip database [" + name + "]", e)));
    }

    //visible for testing
    protected void updateTimestamp(Map<String, Metadata> currentDatabases, String name) {
        Metadata old = currentDatabases.get(name);
        currentDatabases.put(name, new Metadata(System.currentTimeMillis(), old.getFirstChunk(), old.getLastChunk(), old.getMd5()));
        logger.info("geoip database [" + name + "] is up to date, updated timestamp");
        updateTaskState(name);
    }

    boolean updateTaskState(String name) {
        PlainActionFuture<PersistentTask<?>> future = PlainActionFuture.newFuture();
        persistentTask.updatePersistentTaskState(state, future);
        try {
            state = ((GeoIpTaskState) future.actionGet().getState()).copy();
            return true;
        } catch (Exception e) {
            logger.error("can not update state during geoip database [" + name + "] update", e);
            return false;
        }
    }

    //visible for testing
    int indexChunks(String name, InputStream is, int chunk) throws IOException {
        for (byte[] buf = getChunk(is); buf.length != 0; buf = getChunk(is)) {
            client.prepareIndex(DATABASES_INDEX).setId(name + "_" + chunk)
                .setSource(XContentType.SMILE, "name", name, "chunk", chunk, "data", buf)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setWaitForActiveShards(ActiveShardCount.ALL)
                .get();
            chunk++;
        }
        return chunk;
    }

    //visible for testing
    byte[] getChunk(InputStream is) throws IOException {
        byte[] buf = new byte[MAX_CHUNK_SIZE];
        int chunkSize = 0;
        while (chunkSize < MAX_CHUNK_SIZE) {
            int read = is.read(buf, chunkSize, MAX_CHUNK_SIZE - chunkSize);
            if (read == -1) {
                break;
            }
            chunkSize += read;
        }
        if (chunkSize < MAX_CHUNK_SIZE) {
            buf = Arrays.copyOf(buf, chunkSize);
        }
        return buf;
    }

    //for testing only
    void setPersistentTask(AllocatedPersistentTask persistentTask) {
        this.persistentTask = persistentTask;
    }

    //for testing only
    void setState(GeoIpTaskState state) {
        this.state = state;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask allocatedTask, PersistentTaskParams params, PersistentTaskState state) {
        if (scheduled != null) {
            scheduled.cancel();
        }
        if (enabled) {
            this.persistentTask = allocatedTask;
            this.state = state != null ? (GeoIpTaskState) state : new GeoIpTaskState();
            try {
                updateDatabases();
            } catch (Exception e) {
                logger.error("exception during geoip databases update", e);
            }
            scheduleNextRun(pollInterval);
        }
    }

    private void scheduleNextRun(TimeValue time) {
        scheduled = threadPool.schedule(() -> nodeOperation(persistentTask, null, state), time, ThreadPool.Names.GENERIC);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            clusterService.removeListener(this);
            startPersistentTask();
        }
    }

    private void startPersistentTask() {
        ClusterState state = clusterService.state();
        if (state.nodes().isLocalNodeElectedMaster() && PersistentTasksCustomMetadata.getTaskWithId(state, GEOIP_DOWNLOADER) == null) {
            persistentTasksService.sendStartRequest(GEOIP_DOWNLOADER, GEOIP_DOWNLOADER, new GeoIpTaskParams(), ActionListener.wrap(r -> {
            }, e -> {
                if (e instanceof ResourceAlreadyExistsException == false) {
                    logger.error("failed to create geoip downloader task", e);
                    clusterService.addListener(this);
                }
            }));
        }
    }
}
