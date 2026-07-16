/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import net.shibboleth.utilities.java.support.component.ComponentInitializationException;
import net.shibboleth.utilities.java.support.resolver.CriteriaSet;
import net.shibboleth.utilities.java.support.resolver.ResolverException;
import net.shibboleth.utilities.java.support.xml.BasicParserPool;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.security.EntitledFileWatcher;
import org.elasticsearch.xpack.security.support.LogThrottle;
import org.opensaml.core.criterion.EntityIdCriterion;
import org.opensaml.saml.metadata.resolver.impl.AbstractReloadingMetadataResolver;
import org.opensaml.saml.metadata.resolver.impl.FilesystemMetadataResolver;
import org.opensaml.saml.metadata.resolver.impl.HTTPMetadataResolver;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.transport.Transports.assertNotTransportThread;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_METADATA_HTTP_CONNECT_TIMEOUT;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_METADATA_HTTP_FAIL_ON_ERROR;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_METADATA_HTTP_MIN_REFRESH;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_METADATA_HTTP_READ_TIMEOUT;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_METADATA_HTTP_REFRESH;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.IDP_METADATA_PATH;

/**
 * Encapsulates SAML IdP metadata resolution with a cached entity descriptor.
 * After construction, resolution (including blocking HTTP or file I/O) runs on the
 * generic thread pool or the scheduled refresh thread, so the auth hot path can read
 * the cache without blocking on OpenSAML's metadata resolver lock. The constructing
 * thread may run the initial {@link #updateCachedDescriptor()} during {@link #create}.
 * The cache is updated from the resolver periodically (every 60s); the resolver itself
 * is not forced to refresh on that schedule.
 * For HTTP metadata, OpenSAML's resolver refreshes on its own min/max delay.
 * For file metadata, Elasticsearch's {@link FileWatcher} is used to monitor for changes.
 */
final class SamlMetadataResolver implements Releasable, Supplier<EntityDescriptor> {

    private static final Logger logger = LogManager.getLogger(SamlMetadataResolver.class);
    private static final TimeValue REFRESH_INTERVAL = TimeValue.timeValueSeconds(60);
    private static final TimeValue LOGGING_PERIOD = TimeValue.timeValueMinutes(30);

    private final AbstractReloadingMetadataResolver resolver;
    private final String entityId;
    private final String sourceLocation;
    private final boolean failOnError;
    private final ThreadPool threadPool;

    private final EntityDescriptor unresolvedDescriptor;

    private final AtomicReference<EntityDescriptor> cachedEntity = new AtomicReference<>();
    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private final LogThrottle nullCacheWarnThrottle;
    private final LogThrottle refreshFailureLogThrottle;

    private final Collection<Releasable> childReleasables;

    private SamlMetadataResolver(
        AbstractReloadingMetadataResolver resolver,
        String entityId,
        String sourceLocation,
        boolean failOnError,
        ThreadPool threadPool
    ) {
        this.resolver = resolver;
        this.entityId = entityId;
        this.sourceLocation = sourceLocation;
        this.failOnError = failOnError;
        this.threadPool = threadPool;
        this.unresolvedDescriptor = new UnresolvedEntity(entityId, sourceLocation);
        this.nullCacheWarnThrottle = new LogThrottle(threadPool, LOGGING_PERIOD);
        this.refreshFailureLogThrottle = new LogThrottle(threadPool, LOGGING_PERIOD);
        this.childReleasables = new ArrayList<>();
    }

    private record MetadataParseResult(AbstractReloadingMetadataResolver resolver, boolean failOnError, @Nullable Releasable releasable) {}

    /**
     * Create a SAML metadata resolver, perform an initial resolve to populate the cache,
     * and start the 60-second refresh schedule.
     */
    public static SamlMetadataResolver create(
        RealmConfig config,
        SSLService sslService,
        ResourceWatcherService watcherService,
        ThreadPool threadPool
    ) throws ResolverException, ComponentInitializationException, IOException {
        final String metadataPath = SamlRealm.require(config, SamlRealmSettings.IDP_METADATA_PATH);
        if (metadataPath.startsWith("http://")) {
            throw new IllegalArgumentException("The [http] protocol is not supported as it is insecure. Use [https] instead");
        }

        final AtomicReference<SamlMetadataResolver> resolverReference = new AtomicReference<>();
        final MetadataParseResult parseResult;
        boolean forceRefresh = false;
        if (metadataPath.startsWith("https://")) {
            parseResult = parseHttpMetadata(metadataPath, config, sslService);
        } else {
            final Consumer<Path> onFileChanged = path -> {
                final SamlMetadataResolver m = resolverReference.get();
                if (m != null) {
                    m.asyncUpdateCachedDescriptor();
                }
            };
            parseResult = parseFileSystemMetadata(metadataPath, config, watcherService, onFileChanged);
            /* There's a potential race condition between when the OpenSAML file resolver reads the metadata for the first time
             * and when we actively start monitoring the file for changes.
             * If the file is changed in that time we either don't notice, or we discard the event. To avoid that we force
             * a refresh of the metadata after everything is wired up. For a single file read, this is cheap and safe.
             */
            forceRefresh = true;
        }

        final String entityId = SamlRealm.require(config, SamlRealmSettings.IDP_ENTITY_ID);
        final SamlMetadataResolver metadataResolver = new SamlMetadataResolver(
            parseResult.resolver,
            entityId,
            metadataPath,
            parseResult.failOnError,
            threadPool
        );
        resolverReference.set(metadataResolver);

        // Initial resolve to populate cache (resolver already loaded during init, no need to refresh again)
        metadataResolver.updateCachedDescriptor();

        // Start periodic refresh
        final var refreshTask = threadPool.scheduleWithFixedDelay(
            metadataResolver::periodicMetadataRefresh,
            REFRESH_INTERVAL,
            threadPool.generic()
        );
        metadataResolver.addChildReleasable(refreshTask::cancel);
        if (parseResult.releasable != null) {
            metadataResolver.addChildReleasable(parseResult.releasable);
        }

        if (forceRefresh) {
            metadataResolver.forceRefresh();
            metadataResolver.updateCachedDescriptor();
        }
        return metadataResolver;
    }

    public AbstractReloadingMetadataResolver getResolver() {
        return resolver;
    }

    /**
     * Returns the cached IdP {@link EntityDescriptor} when metadata has been resolved successfully.
     * This relies on the background refresh thread, and does not block on OpenSAML or network I/O.
     * If no entity descriptor is available (i.e. the background refresh has failed to resolve one), then this method will
     * <ul>
     * <li>attempt to trigger another background metadata refresh (because the realm is being used so the system should check
     *     whether the resolution problem has been fixed)</li>
     * <li>possibly log a warning that SAML auth will fail until metadata is available (this logging is throttled)</li>
     * <li>return an {@link UnresolvedEntity} stand-in.</li>
     * </ul>
     */
    @Override
    public EntityDescriptor get() {
        final EntityDescriptor descriptor = cachedEntity.get();
        if (descriptor == null) {
            // Trigger an immediate background refresh so a subsequent call might see resolved metadata
            asyncUpdateCachedDescriptor();
            if (nullCacheWarnThrottle.acquire()) {
                logger.warn(
                    "cannot load SAML metadata for [{}] from [{}]; SAML authentication for this realm will fail",
                    entityId,
                    sourceLocation
                );
            }
            return unresolvedDescriptor;
        }
        return descriptor;
    }

    /**
     * Called on a fixed delay on the generic executor. Syncs the cache on this thread (no extra submit).
     */
    private void periodicMetadataRefresh() {
        if (inProgress.compareAndSet(false, true)) {
            try {
                tryUpdateCachedDescriptor(false);
            } catch (Exception e) {
                inProgress.compareAndSet(true, false);
                logger.debug(() -> "SAML metadata periodic refresh failed for [" + entityId + "]", e);
            }
        }
    }

    /**
     * Queues a cache sync on the generic pool (e.g. when {@link #get()} sees a null cache).
     */
    void asyncUpdateCachedDescriptor() {
        if (inProgress.compareAndSet(false, true)) {
            try {
                threadPool.executor(ThreadPool.Names.GENERIC).submit(() -> tryUpdateCachedDescriptor(true));
            } catch (Exception e) {
                inProgress.compareAndSet(true, false);
                logger.debug(() -> "SAML metadata refresh from [" + this.sourceLocation + "] failed for [" + entityId + "]", e);
            }
        }
    }

    /**
     * Forcibly refresh the metadata from the underlying resolver.
     * Happens synchronously on the calling thread.
     * This only refreshes the metadata, it <strong>does not</strong> update the cached entity,
     *   the caller must also call {@link #updateCachedDescriptor()} if that is needed.
     */
    private void forceRefresh() {
        try {
            resolver.refresh();
        } catch (Exception e) {
            logger.debug(
                () -> Strings.format(
                    "SAML metadata unconditional file resync: refresh from [%s] for entity [%s] failed; continuing with cache update",
                    sourceLocation,
                    entityId
                ),
                e
            );
        }
    }

    /**
     * Runs on the generic pool. Syncs the cache from the resolver and clears inProgress when done.
     */
    private void tryUpdateCachedDescriptor(boolean refresh) {
        try {
            if (refresh) {
                forceRefresh();
            }
            updateCachedDescriptor();
        } finally {
            if (inProgress.compareAndSet(true, false) == false) {
                assert false : "Unexpected CAS state: in progress should have been true";
                logger.warn("Unexpected CAS state: in progress should have been true");
            }
        }
    }

    /**
     * Resolves the entity descriptor from the OpenSAML resolver and updates the cache.
     * Runs only on the background thread (or during initial resolve). Never overwrites a real
     * cached entity with an unresolved one.
     */
    void updateCachedDescriptor() {
        try {
            final EntityDescriptor descriptor = resolveEntityDescriptor();
            cachedEntity.set(descriptor);
        } catch (Exception e) {
            // We intentionally don't update the cached entity descriptor here.
            // Most of the time have an out of date descriptor is better than having
            // no descriptor at all - in fact it's entirely possible that it's not out of date,
            // it's just that we couldn't resolve the metadata file on this attempt.
            maybeLog(e);
            if (failOnError) {
                throw ExceptionsHelper.convertToRuntime(e);
            }
        }
    }

    /**
     * Log the exception. It is a warning if we haven't logged in the last {@link #LOGGING_PERIOD}, otherwise it's a debug.
     * This prevents us from filling the logs every time we fail, but also logs errors regularly enough to be noticed
     */
    private void maybeLog(Exception ex) {
        org.apache.logging.log4j.util.Supplier<String> message = () -> Strings.format("Failed to refresh SAML metadata for [%s]", entityId);
        if (refreshFailureLogThrottle.acquire()) {
            logger.warn(message, ex);
        } else {
            logger.debug(message, ex);
        }
    }

    /**
     * Resolves the entity descriptor from the OpenSAML resolver.
     * If the first resolve returns null, forces a resolver refresh and retries once.
     * Returns a non-null {@link EntityDescriptor} on success; otherwise throws (typically
     * {@link ResolverException} or a SAML exception from {@link SamlUtils#samlException}).
     */
    private EntityDescriptor resolveEntityDescriptor() throws ResolverException {
        var criteria = new CriteriaSet(new EntityIdCriterion(entityId));
        EntityDescriptor descriptor = resolver.resolveSingle(criteria);
        ResolverException exception = null;
        if (descriptor == null) {
            try {
                resolver.refresh();
            } catch (ResolverException e) {
                exception = e;
            }
            descriptor = resolver.resolveSingle(criteria);
        }
        if (descriptor != null) {
            return descriptor;
        }
        if (exception != null) {
            throw exception;
        }
        throw SamlUtils.samlException("Cannot find metadata for entity [{}] in [{}]", entityId, sourceLocation);
    }

    @Override
    public void close() {
        Releasables.close(childReleasables);
        resolver.destroy();
    }

    private void addChildReleasable(Releasable releasable) {
        this.childReleasables.add(releasable);
    }

    private static SamlMetadataResolver.MetadataParseResult parseHttpMetadata(String metadataUrl, RealmConfig config, SSLService sslService)
        throws ResolverException, ComponentInitializationException {
        HttpClientBuilder builder = HttpClientBuilder.create();
        final String sslKey = RealmSettings.realmSslPrefix(config.identifier());
        final SslProfile sslProfile = sslService.profile(sslKey);
        final SSLConnectionSocketFactory factory = sslProfile.connectionSocketFactory();
        builder.setSSLSocketFactory(factory);

        TimeValue connectTimeout = config.getSetting(IDP_METADATA_HTTP_CONNECT_TIMEOUT);
        TimeValue readTimeout = config.getSetting(IDP_METADATA_HTTP_READ_TIMEOUT);
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(Math.toIntExact(connectTimeout.millis()))
            .setSocketTimeout(Math.toIntExact(readTimeout.millis()))
            .build();
        builder.setDefaultRequestConfig(requestConfig);

        TimeValue maxRefresh = config.getSetting(IDP_METADATA_HTTP_REFRESH);
        TimeValue minRefresh = config.getSetting(IDP_METADATA_HTTP_MIN_REFRESH);
        if (minRefresh.compareTo(maxRefresh) > 0) {
            if (config.hasSetting(IDP_METADATA_HTTP_MIN_REFRESH)) {
                throw new SettingsException(
                    "the value ({}) for [{}] cannot be greater than the value ({}) for [{}]",
                    minRefresh.getStringRep(),
                    RealmSettings.getFullSettingKey(config, IDP_METADATA_HTTP_MIN_REFRESH),
                    maxRefresh.getStringRep(),
                    RealmSettings.getFullSettingKey(config, IDP_METADATA_HTTP_REFRESH)
                );
            } else {
                minRefresh = maxRefresh;
            }
        }

        HTTPMetadataResolver resolver = new ThreadCheckingHTTPMetadataResolver(builder.build(), metadataUrl);
        resolver.setMinRefreshDelay(Duration.ofMillis(minRefresh.millis()));
        resolver.setMaxRefreshDelay(Duration.ofMillis(maxRefresh.millis()));

        final boolean failOnError = config.getSetting(IDP_METADATA_HTTP_FAIL_ON_ERROR);
        resolver.setFailFastInitialization(failOnError);

        initialiseResolver(resolver, config);

        return new MetadataParseResult(resolver, failOnError, null);
    }

    /**
     * Ensures SAML metadata HTTP fetch does not run on transport worker threads.
     */
    private static final class ThreadCheckingHTTPMetadataResolver extends HTTPMetadataResolver {

        ThreadCheckingHTTPMetadataResolver(final HttpClient client, final String metadataURL) throws ResolverException {
            super(client, metadataURL);
        }

        @Override
        protected byte[] fetchMetadata() throws ResolverException {
            assert assertNotTransportThread("fetching SAML metadata from a URL");
            return super.fetchMetadata();
        }
    }

    @SuppressForbidden(reason = "uses toFile")
    private static MetadataParseResult parseFileSystemMetadata(
        String metadataPath,
        RealmConfig config,
        ResourceWatcherService watcherService,
        Consumer<Path> onFileChange
    ) throws ResolverException, ComponentInitializationException, IOException {
        Objects.requireNonNull(onFileChange);

        final Path path = config.env().configDir().resolve(metadataPath);
        final FilesystemMetadataResolver resolver = new SamlFilesystemMetadataResolver(path.toFile());

        for (var httpSetting : List.of(
            IDP_METADATA_HTTP_REFRESH,
            IDP_METADATA_HTTP_MIN_REFRESH,
            IDP_METADATA_HTTP_FAIL_ON_ERROR,
            IDP_METADATA_HTTP_CONNECT_TIMEOUT,
            IDP_METADATA_HTTP_READ_TIMEOUT
        )) {
            if (config.hasSetting(httpSetting.apply(config.type()))) {
                logger.info(
                    "Ignoring setting [{}] because the IdP metadata is being loaded from a file for SAML realm [{}]",
                    RealmSettings.getFullSettingKey(config, httpSetting.apply(config.type())),
                    config.name()
                );
            }
        }

        final Duration oneDayMs = Duration.ofMillis(TimeValue.timeValueHours(24).millis());
        resolver.setMinRefreshDelay(oneDayMs);
        resolver.setMaxRefreshDelay(oneDayMs);
        initialiseResolver(resolver, config);

        final FileWatcher watcher = new EntitledFileWatcher(path);
        watcher.addListener(new FileChangesListener() {
            @Override
            public void onFileCreated(Path file) {
                onFileChanged(file);
            }

            @Override
            public void onFileDeleted(Path file) {
                onFileChanged(file);
            }

            @Override
            public void onFileChanged(Path file) {
                onFileChange.accept(file);
            }
        });
        var handle = watcherService.add(watcher, ResourceWatcherService.Frequency.MEDIUM);

        return new MetadataParseResult(resolver, true, handle::stop);
    }

    @SuppressForbidden(reason = "uses toFile")
    private static final class SamlFilesystemMetadataResolver extends FilesystemMetadataResolver {

        SamlFilesystemMetadataResolver(final java.io.File metadata) throws ResolverException {
            super(metadata);
        }

        @Override
        protected byte[] fetchMetadata() throws ResolverException {
            assert assertNotTransportThread("fetching SAML metadata from a file");
            return super.fetchMetadata();
        }
    }

    private static void initialiseResolver(AbstractReloadingMetadataResolver resolver, RealmConfig config)
        throws ComponentInitializationException {
        resolver.setRequireValidMetadata(true);
        BasicParserPool pool = new BasicParserPool();
        pool.initialize();
        resolver.setParserPool(pool);
        resolver.setId(config.name());
        try {
            resolver.initialize();
        } catch (ComponentInitializationException e) {
            resolver.destroy();
            throw SamlUtils.samlException("cannot load SAML metadata from [{}]", e, config.getSetting(IDP_METADATA_PATH));
        }
    }
}
