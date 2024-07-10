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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.ingest.geoip.GeoIpTaskState.Metadata;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfigurationMetadata;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.net.PasswordAuthentication;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.geoip.EnterpriseGeoIpDownloaderTaskExecutor.MAXMIND_SETTINGS_PREFIX;

/**
 * Main component responsible for downloading new GeoIP databases.
 * New databases are downloaded in chunks and stored in .geoip_databases index
 * Downloads are verified against MD5 checksum provided by the server
 * Current state of all stored databases is stored in cluster state in persistent task state
 */
public class EnterpriseGeoIpDownloader extends AllocatedPersistentTask {

    private static final Logger logger = LogManager.getLogger(EnterpriseGeoIpDownloader.class);

    // for overriding in tests
    private static final String DEFAULT_MAXMIND_ENDPOINT = System.getProperty(
        MAXMIND_SETTINGS_PREFIX + "endpoint.default",
        "https://download.maxmind.com/geoip/databases"
    );
    // n.b. a future enhancement might be to allow for a MAXMIND_ENDPOINT_SETTING, but
    // at the moment this is an unsupported system property for use in tests (only)

    static String downloadUrl(final String name, final String suffix) {
        String endpointPattern = DEFAULT_MAXMIND_ENDPOINT;
        if (endpointPattern.contains("%")) {
            throw new IllegalArgumentException("Invalid endpoint [" + endpointPattern + "]");
        }
        if (endpointPattern.endsWith("/") == false) {
            endpointPattern += "/";
        }
        endpointPattern += "%s/download?suffix=%s";

        // at this point the pattern looks like this (in the default case):
        // https://download.maxmind.com/geoip/databases/%s/download?suffix=%s

        return Strings.format(endpointPattern, name, suffix);
    }

    static final String DATABASES_INDEX = ".geoip_databases";
    static final int MAX_CHUNK_SIZE = 1024 * 1024;

    private final Client client;
    private final HttpClient httpClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    // visible for testing
    protected volatile EnterpriseGeoIpTaskState state;
    private volatile Scheduler.ScheduledCancellable scheduled;
    private final Supplier<TimeValue> pollIntervalSupplier;
    private final Supplier<HttpClient.PasswordAuthenticationHolder> credentialsSupplier;

    EnterpriseGeoIpDownloader(
        Client client,
        HttpClient httpClient,
        ClusterService clusterService,
        ThreadPool threadPool,
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers,
        Supplier<TimeValue> pollIntervalSupplier,
        Supplier<HttpClient.PasswordAuthenticationHolder> credentialsSupplier
    ) {
        super(id, type, action, description, parentTask, headers);
        this.client = client;
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.pollIntervalSupplier = pollIntervalSupplier;
        this.credentialsSupplier = credentialsSupplier;
    }

    void setState(EnterpriseGeoIpTaskState state) {
        // this is for injecting the state in GeoIpDownloaderTaskExecutor#nodeOperation just after the task instance has been created
        // by the PersistentTasksNodeService -- since the GeoIpDownloader is newly created, the state will be null, and the passed-in
        // state cannot be null
        assert this.state == null;
        assert state != null;
        this.state = state;
    }

    // visible for testing
    void updateDatabases() throws IOException {
        var clusterState = clusterService.state();
        var geoipIndex = clusterState.getMetadata().getIndicesLookup().get(EnterpriseGeoIpDownloader.DATABASES_INDEX);
        if (geoipIndex != null) {
            logger.trace("The {} index is not null", EnterpriseGeoIpDownloader.DATABASES_INDEX);
            if (clusterState.getRoutingTable().index(geoipIndex.getWriteIndex()).allPrimaryShardsActive() == false) {
                throw new ElasticsearchException("not all primary shards of [" + DATABASES_INDEX + "] index are active");
            }
            var blockException = clusterState.blocks().indexBlockedException(ClusterBlockLevel.WRITE, geoipIndex.getWriteIndex().getName());
            if (blockException != null) {
                throw blockException;
            }
        }

        logger.trace("Updating geoip databases");
        IngestGeoIpMetadata geoIpMeta = clusterState.metadata().custom(IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.EMPTY);

        // if there are entries in the cs that aren't in the persistent task state,
        // then download those (only)
        // ---
        // if there are in the persistent task state, that aren't in the cluster state
        // then nuke those (only)
        // ---
        // else, just download everything
        boolean addedSomething = false;
        {
            EnterpriseGeoIpTaskState _state = state;
            Set<String> metas = Set.copyOf(_state.getDatabases().keySet());
            for (Map.Entry<String, DatabaseConfigurationMetadata> entry : geoIpMeta.getDatabases().entrySet()) {

                final String id = entry.getKey();
                DatabaseConfiguration database = entry.getValue().database();
                if (metas.contains(database.name() + ".mmdb") == false) {
                    logger.info("A new database appeared! [{}]", database.name());

                    try (HttpClient.PasswordAuthenticationHolder holder = credentialsSupplier.get()) {
                        processDatabase(holder.get(), id, database);
                    }

                    addedSomething = true;
                }
            }
        }

        boolean droppedSomething = false;
        {
            // rip anything out of the task state that doesn't match what's in the cluster state,
            // that is, if there's no longer an entry for a database in the repository,
            // then drop it from the task state, too
            Set<String> databases = geoIpMeta.getDatabases()
                .values()
                .stream()
                .map(c -> c.database().name() + ".mmdb")
                .collect(Collectors.toSet());
            EnterpriseGeoIpTaskState _state = state;
            Collection<Map.Entry<String, Metadata>> metas = List.copyOf(_state.getDatabases().entrySet());
            for (Map.Entry<String, Metadata> entry : metas) {
                String name = entry.getKey();
                Metadata meta = entry.getValue();
                if (databases.contains(name) == false) {
                    logger.info("Dropping [{}], databases was {}", name, databases);
                    _state = _state.remove(name);
                    deleteOldChunks(name, meta.lastChunk() + 1);
                    droppedSomething = true;
                }
            }
            state = _state;
            updateTaskState();
        }

        if (addedSomething == false && droppedSomething == false) {
            for (Map.Entry<String, DatabaseConfigurationMetadata> entry : geoIpMeta.getDatabases().entrySet()) {
                final String id = entry.getKey();
                DatabaseConfiguration database = entry.getValue().database();

                try (HttpClient.PasswordAuthenticationHolder holder = credentialsSupplier.get()) {
                    processDatabase(holder.get(), id, database);
                }
            }
        }
    }

    void processDatabase(PasswordAuthentication auth, String id, DatabaseConfiguration database) throws IOException {
        final String name = database.name();
        logger.info("Lol, off we go, downloading {} / {}", id, name);

        final String sha256Url = downloadUrl(name, "tar.gz.sha256");
        final String tgzUrl = downloadUrl(name, "tar.gz");

        final Pattern checksumPattern = Pattern.compile("(\\w{64})\\s\\s(.*)");
        String result = new String(httpClient.getBytes(auth, sha256Url), StandardCharsets.UTF_8).trim(); // this throws if the auth is bad
        var matcher = checksumPattern.matcher(result);
        boolean match = matcher.matches(); // TODO this better be true!
        final String sha256 = matcher.group(1); // no match found!?

        logger.info("off to the races! [{} / {}]", id, name);
        logger.info("sha256 was [{}]", sha256);

        processDatabase(auth, name + ".mmdb" /* TODO ugh */, sha256, tgzUrl);
    }

    private void processDatabase(PasswordAuthentication auth, String name, String sha256, String url) {
        Metadata metadata = state.getDatabases().getOrDefault(name, Metadata.EMPTY);
        if (Objects.equals(metadata.sha256(), sha256)) {
            updateTimestamp(name, metadata);
            return;
        }
        logger.debug("downloading geoip database [{}]", name);
        long start = System.currentTimeMillis();
        try (InputStream is = httpClient.get(auth, url)) {
            int firstChunk = metadata.lastChunk() + 1; // if there is no metadata, then Metadata.EMPTY + 1 = 0
            Tuple<Integer, String> tuple = indexChunks(name, is, firstChunk, MessageDigests.sha256(), sha256, start);
            int lastChunk = tuple.v1();
            String md5 = tuple.v2();
            if (lastChunk > firstChunk) {
                state = state.put(name, new Metadata(start, firstChunk, lastChunk - 1, md5, start, sha256));
                updateTaskState();
                logger.info("successfully downloaded geoip database [{}]", name);
                deleteOldChunks(name, firstChunk);
            }
        } catch (Exception e) {
            logger.error(() -> "error downloading geoip database [" + name + "]", e);
        }
    }

    // visible for testing
    void deleteOldChunks(String name, int firstChunk) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder().filter(new MatchQueryBuilder("name", name))
            .filter(new RangeQueryBuilder("chunk").to(firstChunk, false));
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        request.indices(DATABASES_INDEX);
        request.setQuery(queryBuilder);
        client.execute(
            DeleteByQueryAction.INSTANCE,
            request,
            ActionListener.wrap(r -> {}, e -> logger.warn("could not delete old chunks for geoip database [" + name + "]", e))
        );
    }

    // visible for testing
    protected void updateTimestamp(String name, Metadata old) {
        logger.debug("geoip database [{}] is up to date, updated timestamp", name);
        state = state.put(
            name,
            new Metadata(old.lastUpdate(), old.firstChunk(), old.lastChunk(), old.md5(), System.currentTimeMillis(), old.sha256())
        );
        updateTaskState();
    }

    void updateTaskState() {
        PlainActionFuture<PersistentTask<?>> future = new PlainActionFuture<>();
        updatePersistentTaskState(state, future);
        state = ((EnterpriseGeoIpTaskState) future.actionGet().getState());
    }

    // visible for testing
    Tuple<Integer, String> indexChunks(
        String name,
        InputStream is,
        int chunk,
        @Nullable MessageDigest digest,
        String expectedChecksum,
        long timestamp
    ) throws IOException {
        MessageDigest md5 = MessageDigests.md5();
        for (byte[] buf = getChunk(is); buf.length != 0; buf = getChunk(is)) {
            md5.update(buf);
            if (digest != null) {
                digest.update(buf);
            }
            IndexRequest indexRequest = new IndexRequest(DATABASES_INDEX).id(name + "_" + chunk + "_" + timestamp)
                .create(true)
                .source(XContentType.SMILE, "name", name, "chunk", chunk, "data", buf);
            client.index(indexRequest).actionGet();
            chunk++;
        }

        // May take some time before automatic flush kicks in:
        // (otherwise the translog will contain large documents for some time without good reason)
        FlushRequest flushRequest = new FlushRequest(DATABASES_INDEX);
        client.admin().indices().flush(flushRequest).actionGet();
        // Ensure that the chunk documents are visible:
        RefreshRequest refreshRequest = new RefreshRequest(DATABASES_INDEX);
        client.admin().indices().refresh(refreshRequest).actionGet();

        String actualMd5 = MessageDigests.toHexString(md5.digest());
        String actualChecksum = digest == null ? actualMd5 : MessageDigests.toHexString(digest.digest());
        if (Objects.equals(expectedChecksum, actualChecksum) == false) {
            throw new IOException("md5 checksum mismatch, expected [" + expectedChecksum + "], actual [" + actualChecksum + "]");
        }
        return Tuple.tuple(chunk, actualMd5);
    }

    // visible for testing
    static byte[] getChunk(InputStream is) throws IOException {
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

    /**
     * Downloads the geoip databases now, and schedules them to be downloaded again after pollInterval.
     */
    void runDownloader() {
        // by the time we reach here, the state will never be null
        assert state != null;

        logger.info("EnterpriseGeoIpDownloader#runDownloader");
        if (isCancelled() || isCompleted()) {
            logger.info("EnterpriseGeoIpDownloader#runDownloader -- isCancelled or isCompleted, I'm out! bye!");
            return;
        }
        try {
            updateDatabases();
        } catch (Exception e) {
            logger.error("exception during geoip databases update", e);
        }
        try {
            cleanDatabases();
        } catch (Exception e) {
            logger.error("exception during geoip databases cleanup", e);
        }

        // TODO we almost certainly need to do something more clever here
        // i like the idea of checking the lowest last-checked time and then running the math to get
        // to the next interval from then
        scheduleNextRun(pollIntervalSupplier.get());
    }

    /**
     * This method requests that the downloader be rescheduled to run immediately (presumably because a dynamic property supplied by
     * pollIntervalSupplier or eagerDownloadSupplier has changed, or a pipeline with a geoip processor has been added). This method does
     * nothing if this task is cancelled, completed, or has not yet been scheduled to run for the first time. It cancels any existing
     * scheduled run.
     */
    public void requestReschedule() {
        logger.info("EnterpriseGeoIpDownloader#requestReschedule");
        if (isCancelled() || isCompleted()) {
            logger.info("EnterpriseGeoIpDownloader#requestReschedule -- isCancelled or isCompleted, I'm out! bye!");
            return;
        }
        if (scheduled != null && scheduled.cancel()) {
            scheduleNextRun(TimeValue.ZERO);
        }
    }

    private void cleanDatabases() {
        // this cleanDatabases logic is wrong, it'll deleteOldChunks() repeatedly forever,
        // and it counts down the lastCheck a millisecond at a time which seems like nonsense to me.
        // ALSO, we shouldn't use peek for this!!!!!
        long expiredDatabases = state.getDatabases()
            .entrySet()
            .stream()
            .filter(e -> e.getValue().isValid(clusterService.state().metadata().settings()) == false)
            .peek(e -> {
                String name = e.getKey();
                Metadata meta = e.getValue();
                deleteOldChunks(name, meta.lastChunk() + 1);
                state = state.put(
                    name,
                    // WAT, it's very wrong that this is doing lastCheck - 1... it seems like nonsense!
                    new Metadata(meta.lastUpdate(), meta.firstChunk(), meta.lastChunk(), meta.md5(), meta.lastCheck() - 1, meta.sha256())
                );
                updateTaskState();
            })
            .count();
    }

    @Override
    protected void onCancelled() {
        logger.info("EnterpriseGeoIpDownloader#onCancelled");
        if (scheduled != null) {
            logger.info("EnterpriseGeoIpDownloader#onCancelled -- calling cancel!");
            scheduled.cancel();
        }
        markAsCompleted();
    }

    private void scheduleNextRun(TimeValue time) {
        if (threadPool.scheduler().isShutdown() == false) {
            scheduled = threadPool.schedule(this::runDownloader, time, threadPool.generic());
        }
    }

}
