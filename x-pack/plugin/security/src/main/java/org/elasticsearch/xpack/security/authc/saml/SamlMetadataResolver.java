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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.security.PrivilegedFileWatcher;
import org.opensaml.core.criterion.EntityIdCriterion;
import org.opensaml.saml.metadata.resolver.impl.AbstractReloadingMetadataResolver;
import org.opensaml.saml.metadata.resolver.impl.FilesystemMetadataResolver;
import org.opensaml.saml.metadata.resolver.impl.HTTPMetadataResolver;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivilegedActionException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
 * Resolution (including blocking HTTP or file I/O) runs only on a background thread,
 * so the auth hot path can read the cache without blocking on OpenSAML's metadata
 * resolver lock. The cache is updated from the resolver periodically (every 60s);
 * the resolver itself is not forced to refresh on that schedule. For HTTP metadata,
 * OpenSAML's resolver refreshes on its own min/max delay; for file metadata, the
 * file watcher calls {@code resolver.refresh()} on change.
 */
final class SamlMetadataResolver implements Releasable, Supplier<EntityDescriptor> {

    private static final Logger logger = LogManager.getLogger(SamlMetadataResolver.class);
    private static final TimeValue REFRESH_INTERVAL = TimeValue.timeValueSeconds(60);
    private static final long LOGGING_PERIOD_MS = TimeValue.timeValueMinutes(30).millis();

    private final AbstractReloadingMetadataResolver resolver;
    private final String entityId;
    private final String sourceLocation;
    private final boolean failOnError;
    private final ThreadPool threadPool;

    private final EntityDescriptor unresolvedDescriptor;

    private final AtomicReference<EntityDescriptor> cachedEntity = new AtomicReference<>();
    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private volatile long lastErrorLogMs = -1;

    private volatile Scheduler.Cancellable refreshTask;

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
    }

    /**
     * Create a SAML metadata resolver, perform an initial resolve to populate the cache,
     * and start the 60-second refresh schedule.
     */
    public static SamlMetadataResolver create(
        Logger logger,
        RealmConfig config,
        SSLService sslService,
        ResourceWatcherService watcherService,
        ThreadPool threadPool
    ) throws ResolverException, ComponentInitializationException, PrivilegedActionException, IOException {
        final String metadataPath = SamlRealm.require(config, SamlRealmSettings.IDP_METADATA_PATH);
        if (metadataPath.startsWith("http://")) {
            throw new IllegalArgumentException("The [http] protocol is not supported as it is insecure. Use [https] instead");
        }
        final Tuple<AbstractReloadingMetadataResolver, Boolean> tuple;
        if (metadataPath.startsWith("https://")) {
            tuple = parseHttpMetadata(metadataPath, config, sslService);
        } else {
            tuple = parseFileSystemMetadata(logger, metadataPath, config, watcherService);
        }
        final AbstractReloadingMetadataResolver resolver = tuple.v1();
        final boolean failOnError = tuple.v2();

        final String entityId = SamlRealm.require(config, SamlRealmSettings.IDP_ENTITY_ID);
        final SamlMetadataResolver metadataResolver = new SamlMetadataResolver(resolver, entityId, metadataPath, failOnError, threadPool);

        // Initial resolve to populate cache (resolver already loaded during init, no need to refresh again)
        metadataResolver.updateCachedDescriptor();

        // Start periodic refresh
        metadataResolver.refreshTask = threadPool.scheduleWithFixedDelay(
            metadataResolver::asyncUpdateCachedDescriptor,
            REFRESH_INTERVAL,
            threadPool.generic()
        );

        return metadataResolver;
    }

    public AbstractReloadingMetadataResolver getResolver() {
        return resolver;
    }

    @Override
    public EntityDescriptor get() {
        final EntityDescriptor descriptor = cachedEntity.get();
        if (descriptor == null) {
            // Trigger an immediate background refresh so a subsequent call might see resolved metadata
            asyncUpdateCachedDescriptor();
            logger.warn(
                "cannot load SAML metadata for [{}] from [{}]; SAML authentication for this realm will fail",
                entityId,
                sourceLocation
            );
            return unresolvedDescriptor;
        }
        return descriptor;
    }

    /**
     * Called by the scheduled task every {@link #REFRESH_INTERVAL} seconds.
     * Submits a runnable to the generic pool that syncs the cache from the resolver.
     * If the interval has elapsed and no run is already in progress, the runnable is submitted.
     */
    void asyncUpdateCachedDescriptor() {
        if (inProgress.compareAndSet(false, true)) {
            try {
                threadPool.executor(ThreadPool.Names.GENERIC).submit(this::tryUpdateCachedDescriptor);
            } catch (Exception e) {
                inProgress.compareAndSet(true, false);
                logger.debug(() -> "SAML metadata refresh failed for [" + entityId + "]", e);
            }
        }
    }

    /**
     * Runs on the generic pool. Syncs the cache from the resolver and clears inProgress when done.
     */
    private void tryUpdateCachedDescriptor() {
        try {
            updateCachedDescriptor();
        } finally {
            if (inProgress.compareAndSet(true, false) == false) {
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
     * Log the exception. It is a warning if we haven't logged in the last {@link #LOGGING_PERIOD_MS}, otherwise it's a debug.
     * This prevents us from filling the logs every time we fail, but also logs errors regularly enough to be noticed
     */
    private void maybeLog(Exception ex) {
        final Supplier<String> msg = () -> Strings.format("Failed to refresh SAML metadata for [%s]", entityId);
        final long now = threadPool.relativeTimeInMillis();
        if (lastErrorLogMs < 0 || now - lastErrorLogMs > LOGGING_PERIOD_MS) {
            logger.warn(msg, ex);
            lastErrorLogMs = now;
        } else {
            logger.debug(msg, ex);
        }
    }

    /**
     * Resolves the entity descriptor from the resolver. If the first resolve returns null,
     * forces a resolver refresh and retries once (same "refresh when null" behavior as the original code).
     * Returns a tuple of (descriptor, errorToLog). The descriptor is a real EntityDescriptor,
     * UnresolvedEntity, or null on failure. The errorToLog is the most important exception to log
     * (e.g. a ResolverException from refresh); log it via maybeLog in the caller.
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
        if (refreshTask != null) {
            refreshTask.cancel();
        }
        resolver.destroy();
    }

    private static Tuple<AbstractReloadingMetadataResolver, Boolean> parseHttpMetadata(
        String metadataUrl,
        RealmConfig config,
        SSLService sslService
    ) throws ResolverException, ComponentInitializationException {
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

        HTTPMetadataResolver resolver = new PrivilegedHTTPMetadataResolver(builder.build(), metadataUrl);
        resolver.setMinRefreshDelay(Duration.ofMillis(minRefresh.millis()));
        resolver.setMaxRefreshDelay(Duration.ofMillis(maxRefresh.millis()));

        final boolean failOnError = config.getSetting(IDP_METADATA_HTTP_FAIL_ON_ERROR);
        resolver.setFailFastInitialization(failOnError);

        initialiseResolver(resolver, config);

        return new Tuple<>(resolver, failOnError);
    }

    private static final class PrivilegedHTTPMetadataResolver extends HTTPMetadataResolver {

        PrivilegedHTTPMetadataResolver(final HttpClient client, final String metadataURL) throws ResolverException {
            super(client, metadataURL);
        }

        @Override
        protected byte[] fetchMetadata() throws ResolverException {
            assert assertNotTransportThread("fetching SAML metadata from a URL");
            return PrivilegedHTTPMetadataResolver.super.fetchMetadata();
        }
    }

    @SuppressForbidden(reason = "uses toFile")
    private static Tuple<AbstractReloadingMetadataResolver, Boolean> parseFileSystemMetadata(
        Logger logger,
        String metadataPath,
        RealmConfig config,
        ResourceWatcherService watcherService
    ) throws ResolverException, ComponentInitializationException, IOException, PrivilegedActionException {

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
                    "Ignoring setting [{}] because the IdP metadata is being loaded from a file",
                    RealmSettings.getFullSettingKey(config, httpSetting.apply(config.type()))
                );
            }
        }

        final Duration oneDayMs = Duration.ofMillis(TimeValue.timeValueHours(24).millis());
        resolver.setMinRefreshDelay(oneDayMs);
        resolver.setMaxRefreshDelay(oneDayMs);
        initialiseResolver(resolver, config);

        FileWatcher watcher = new PrivilegedFileWatcher(path);
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
                try {
                    resolver.refresh();
                } catch (Exception e) {
                    logger.warn(() -> "An error occurred while reloading file [" + file + "]", e);
                }
            }
        });
        watcherService.add(watcher, ResourceWatcherService.Frequency.MEDIUM);

        return new Tuple<>(resolver, true);
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
