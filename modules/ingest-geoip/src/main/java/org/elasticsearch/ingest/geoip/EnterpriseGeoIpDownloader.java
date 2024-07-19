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
import org.elasticsearch.ExceptionsHelper;
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
import java.util.function.Function;
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
    private static final Pattern CHECKSUM_PATTERN = Pattern.compile("(\\w{64})\\s\\s(.*)");

    // for overriding in tests
    static String DEFAULT_MAXMIND_ENDPOINT = System.getProperty(
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
    private final Function<String, HttpClient.PasswordAuthenticationHolder> credentialsBuilder;

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
        Function<String, HttpClient.PasswordAuthenticationHolder> credentialsBuilder
    ) {
        super(id, type, action, description, parentTask, headers);
        this.client = client;
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.pollIntervalSupplier = pollIntervalSupplier;
        this.credentialsBuilder = credentialsBuilder;
    }

    void setState(EnterpriseGeoIpTaskState state) {
        // this is for injecting the state in GeoIpDownloaderTaskExecutor#nodeOperation just after the task instance has been created
        // by the PersistentTasksNodeService -- since the GeoIpDownloader is newly created, the state will be null, and the passed-in
        // state cannot be null
        assert this.state == null
            : "setState() cannot be called when state is already non-null. This most likely happened because setState() was called twice";
        assert state != null : "Should never call setState with a null state. Pass an EnterpriseGeoIpTaskState.EMPTY instead.";
        this.state = state;
    }

    // visible for testing
    void updateDatabases() throws IOException {
        var clusterState = clusterService.state();
        var geoipIndex = clusterState.getMetadata().getIndicesLookup().get(EnterpriseGeoIpDownloader.DATABASES_INDEX);
        if (geoipIndex != null) {
            logger.trace("the geoip index [{}] exists", EnterpriseGeoIpDownloader.DATABASES_INDEX);
            if (clusterState.getRoutingTable().index(geoipIndex.getWriteIndex()).allPrimaryShardsActive() == false) {
                logger.debug("not updating databases because not all primary shards of [{}] index are active yet", DATABASES_INDEX);
                return;
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
            Set<String> existingDatabaseNames = state.getDatabases().keySet();
            for (Map.Entry<String, DatabaseConfigurationMetadata> entry : geoIpMeta.getDatabases().entrySet()) {
                final String id = entry.getKey();
                DatabaseConfiguration database = entry.getValue().database();
                if (existingDatabaseNames.contains(database.name() + ".mmdb") == false) {
                    logger.debug("A new database appeared [{}]", database.name());

                    final String accountId = database.maxmind().accountId();
                    try (HttpClient.PasswordAuthenticationHolder holder = credentialsBuilder.apply(accountId)) {
                        if (holder == null) {
                            logger.warn("No credentials found to download database [{}], skipping download...", id);
                        } else {
                            processDatabase(holder.get(), database);
                            addedSomething = true;
                        }
                    }
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
            Collection<Tuple<String, Metadata>> metas = _state.getDatabases()
                .entrySet()
                .stream()
                .map(entry -> Tuple.tuple(entry.getKey(), entry.getValue()))
                .toList();
            for (Tuple<String, Metadata> metaTuple : metas) {
                String name = metaTuple.v1();
                Metadata meta = metaTuple.v2();
                if (databases.contains(name) == false) {
                    logger.debug("Dropping [{}], databases was {}", name, databases);
                    _state = _state.remove(name);
                    deleteOldChunks(name, meta.lastChunk() + 1);
                    droppedSomething = true;
                }
            }
            if (droppedSomething) {
                state = _state;
                updateTaskState();
            }
        }

        if (addedSomething == false && droppedSomething == false) {
            RuntimeException accumulator = null;
            for (Map.Entry<String, DatabaseConfigurationMetadata> entry : geoIpMeta.getDatabases().entrySet()) {
                final String id = entry.getKey();
                DatabaseConfiguration database = entry.getValue().database();

                final String accountId = database.maxmind().accountId();
                try (HttpClient.PasswordAuthenticationHolder holder = credentialsBuilder.apply(accountId)) {
                    if (holder == null) {
                        logger.warn("No credentials found to download database [{}], skipping download...", id);
                    } else {
                        processDatabase(holder.get(), database);
                    }
                } catch (Exception e) {
                    accumulator = ExceptionsHelper.useOrSuppress(accumulator, ExceptionsHelper.convertToRuntime(e));
                }
            }
            if (accumulator != null) {
                throw accumulator;
            }
        }
    }

    /**
     * This method fetches the sha256 file and tar.gz file for the given database from the Maxmind endpoint, then indexes that tar.gz
     * file into the .geoip_databases Elasticsearch index, deleting any old versions of the database tar.gz from the index if they exist.
     * If the computed sha256 does not match the expected sha256, an error will be logged and the database will not be put into the
     * Elasticsearch index.
     * <p>
     * As an implementation detail, this method retrieves the sha256 checksum of the database to download and then invokes
     * {@link EnterpriseGeoIpDownloader#processDatabase(PasswordAuthentication, String, String, String)} with that checksum, deferring to
     * that method to actually download and process the tar.gz itself.
     *
     * @param auth The credentials to use to download from the Maxmind endpoint
     * @param database The database to be downloaded from Maxmind and indexed into an Elasticsearch index
     * @throws IOException If there is an error fetching the sha256 file
     */
    void processDatabase(PasswordAuthentication auth, DatabaseConfiguration database) throws IOException {
        final String name = database.name();
        logger.debug("Processing database [{}] for configuration [{}]", name, database.id());

        final String sha256Url = downloadUrl(name, "tar.gz.sha256");
        final String tgzUrl = downloadUrl(name, "tar.gz");

        String result = new String(httpClient.getBytes(auth, sha256Url), StandardCharsets.UTF_8).trim(); // this throws if the auth is bad
        var matcher = CHECKSUM_PATTERN.matcher(result);
        boolean match = matcher.matches();
        if (match == false) {
            throw new RuntimeException("Unexpected sha256 response from [" + sha256Url + "]");
        }
        final String sha256 = matcher.group(1);
        // the name that comes from the enterprise downloader cluster state doesn't include the .mmdb extension,
        // but the downloading and indexing of database code expects it to be there, so we add it on here before further processing
        processDatabase(auth, name + ".mmdb", sha256, tgzUrl);
    }

    /**
     * This method fetches the tar.gz file for the given database from the Maxmind endpoint, then indexes that tar.gz
     * file into the .geoip_databases Elasticsearch index, deleting any old versions of the database tar.gz from the index if they exist.
     *
     * @param auth The credentials to use to download from the Maxmind endpoint
     * The name of the database to be downloaded from Maxmind and indexed into an Elasticsearch index
     * @param sha256 The sha256 to compare to the computed sha256 of the downloaded tar.gz file
     * @param url The URL for the Maxmind endpoint from which the database's tar.gz will be downloaded
     */
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
            throw new IOException("checksum mismatch, expected [" + expectedChecksum + "], actual [" + actualChecksum + "]");
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
    synchronized void runDownloader() {
        // by the time we reach here, the state will never be null
        assert this.state != null : "this.setState() is null. You need to call setState() before calling runDownloader()";

        // there's a race condition between here and requestReschedule. originally this scheduleNextRun call was at the end of this
        // block, but remember that updateDatabases can take seconds to run (it's downloading bytes from the internet), and so during the
        // very first run there would be no future run scheduled to reschedule in requestReschedule. which meant that if you went from zero
        // to N(>=2) databases in quick succession, then all but the first database wouldn't necessarily get downloaded, because the
        // requestReschedule call in the EnterpriseGeoIpDownloaderTaskExecutor's clusterChanged wouldn't have a scheduled future run to
        // reschedule. scheduling the next run at the beginning of this run means that there's a much smaller window (milliseconds?, rather
        // than seconds) in which such a race could occur. technically there's a window here, still, but i think it's _greatly_ reduced.
        scheduleNextRun(pollIntervalSupplier.get());
        // TODO regardless of the above comment, i like the idea of checking the lowest last-checked time and then running the math to get
        // to the next interval from then -- maybe that's a neat future enhancement to add

        if (isCancelled() || isCompleted()) {
            return;
        }
        try {
            updateDatabases(); // n.b. this downloads bytes from the internet, it can take a while
        } catch (Exception e) {
            logger.error("exception during geoip databases update", e);
        }
        try {
            cleanDatabases();
        } catch (Exception e) {
            logger.error("exception during geoip databases cleanup", e);
        }
    }

    /**
     * This method requests that the downloader be rescheduled to run immediately (presumably because a dynamic property supplied by
     * pollIntervalSupplier or eagerDownloadSupplier has changed, or a pipeline with a geoip processor has been added). This method does
     * nothing if this task is cancelled, completed, or has not yet been scheduled to run for the first time. It cancels any existing
     * scheduled run.
     */
    public void requestReschedule() {
        if (isCancelled() || isCompleted()) {
            return;
        }
        if (scheduled != null && scheduled.cancel()) {
            scheduleNextRun(TimeValue.ZERO);
        }
    }

    private void cleanDatabases() {
        List<Tuple<String, Metadata>> expiredDatabases = state.getDatabases()
            .entrySet()
            .stream()
            .filter(e -> e.getValue().isNewEnough(clusterService.state().metadata().settings()) == false)
            .map(entry -> Tuple.tuple(entry.getKey(), entry.getValue()))
            .toList();
        expiredDatabases.forEach(e -> {
            String name = e.v1();
            Metadata meta = e.v2();
            deleteOldChunks(name, meta.lastChunk() + 1);
            state = state.put(name, new Metadata(meta.lastUpdate(), meta.firstChunk(), meta.lastChunk(), meta.md5(), meta.lastCheck() - 1));
            updateTaskState();
        });
    }

    @Override
    protected void onCancelled() {
        if (scheduled != null) {
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
