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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.EnterpriseGeoIpTask.EnterpriseGeoIpTaskParams;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.ingest.EnterpriseGeoIpTask.ENTERPRISE_GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor.ENABLED_SETTING;
import static org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING;

public class EnterpriseGeoIpDownloaderTaskExecutor extends PersistentTasksExecutor<EnterpriseGeoIpTaskParams>
    implements
        ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(EnterpriseGeoIpDownloader.class);

    private static final String MAXMIND_SETTINGS_PREFIX = "ingest.geoip.downloader.maxmind.";

    public static final Setting<String> MAXMIND_DEFAULT_ACCOUNT_ID_SETTING = Setting.simpleString(
        MAXMIND_SETTINGS_PREFIX + "default.account_id",
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<SecureString> MAXMIND_DEFAULT_LICENSE_KEY_SETTING = SecureSetting.secureString(
        MAXMIND_SETTINGS_PREFIX + "default.license_key",
        null
    );

    private final Client client;
    private final HttpClient httpClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Settings settings;
    private volatile TimeValue pollInterval;
    private final AtomicBoolean taskIsBootstrapped = new AtomicBoolean(false);
    private final AtomicReference<EnterpriseGeoIpDownloader> currentTask = new AtomicReference<>();

    private volatile String defaultMaxmindAccountId;
    private volatile SecureSettings cachedSecureSettings;

    EnterpriseGeoIpDownloaderTaskExecutor(Client client, HttpClient httpClient, ClusterService clusterService, ThreadPool threadPool) {
        // this registers that the 'geoip-downloader' is the kind of task this thing creates, that is, we could create many of them,
        // but we happen to only create just the one ('name' is a bit like a 'kind', versus 'id' which identifies a specific one)
        super(ENTERPRISE_GEOIP_DOWNLOADER, threadPool.generic());
        this.client = new OriginSettingClient(client, IngestService.INGEST_ORIGIN);
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.settings = clusterService.getSettings(); // the javadocs say "the node's settings" -- this is interesting
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings); // TODO right we should watch for changes on this

        // grab the account id from the node settings on startup
        setDefaultMaxmindAccountId(MAXMIND_DEFAULT_ACCOUNT_ID_SETTING.get(clusterService.getSettings()));
        // and do an initial load using the node settings as well
        reload(clusterService.getSettings());
    }

    /**
     * This method completes the initialization of the EnterpriseGeoIpDownloaderTaskExecutor by registering several listeners.
     */
    public void init() {
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAXMIND_DEFAULT_ACCOUNT_ID_SETTING, this::setDefaultMaxmindAccountId);
    }

    private void setDefaultMaxmindAccountId(String defaultMaxmindAccountId) {
        this.defaultMaxmindAccountId = defaultMaxmindAccountId;
    }

    private HttpClient.PasswordAuthenticationHolder buildCredentials() {
        return new HttpClient.PasswordAuthenticationHolder(
            this.defaultMaxmindAccountId,
            cachedSecureSettings.getString(MAXMIND_DEFAULT_LICENSE_KEY_SETTING.getKey()).getChars()
        );
    }

    @Override
    protected EnterpriseGeoIpDownloader createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<EnterpriseGeoIpTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        return new EnterpriseGeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            id,
            type,
            action,
            getDescription(taskInProgress),
            parentTaskId,
            headers,
            () -> pollInterval,
            this::buildCredentials
        );
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, EnterpriseGeoIpTaskParams params, PersistentTaskState state) {
        // TODO so we'd want to override createTask and have our own AllocatedPersistentTask and its associated state,
        // but this is enough to prove the principle in the meantime.
        logger.info("Running enterprise downloader, state was [{}]", state);

        // this runs on the node that was allocated to have the task,
        // and runDownloader uses a scheduler to schedule its own next run,
        // so this kicks things off if the thing is enabled
        EnterpriseGeoIpDownloader downloader = (EnterpriseGeoIpDownloader) task;
        EnterpriseGeoIpTaskState geoIpTaskState = (state == null) ? EnterpriseGeoIpTaskState.EMPTY : (EnterpriseGeoIpTaskState) state;
        downloader.setState(geoIpTaskState);
        currentTask.set(downloader);
        // this double-settings check is very unusual
        if (ENABLED_SETTING.get(clusterService.state().metadata().settings(), settings)) {
            downloader.runDownloader();
        }
    }

    public EnterpriseGeoIpDownloader getCurrentTask() {
        return currentTask.get();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        EnterpriseGeoIpDownloader currentDownloader = getCurrentTask();
        if (currentDownloader != null) {
            boolean hasGeoIpMetadataChanges = event.changedCustomMetadataSet().contains(IngestGeoIpMetadata.TYPE);
            if (hasGeoIpMetadataChanges) {
                logger.info("Something changed in (the custom metadata of) the cluster state, re-running the downloader now");
                currentDownloader.requestReschedule(); // watching the cluster changed events to kick the thing off if it's not running
            }
        }
    }

    public synchronized void reload(Settings settings) {
        // `SecureSettings` are available here! cache them as they will be needed
        // whenever dynamic cluster settings change and we have to rebuild the accounts
        try {
            this.cachedSecureSettings = extractSecureSettings(settings, List.of(MAXMIND_DEFAULT_LICENSE_KEY_SETTING));
        } catch (GeneralSecurityException e) {
            logger.error("Keystore exception while reloading enterprise geoip download task executor", e);
            return;
        }
    }

    /**
     * Extracts the {@link SecureSettings}` out of the passed in {@link Settings} object. The {@code Setting} argument has to have the
     * {@code SecureSettings} open/available. Normally {@code SecureSettings} are available only under specific callstacks (eg. during node
     * initialization or during a `reload` call). The returned copy can be reused freely as it will never be closed (this is a bit of
     * cheating, but it is necessary in this specific circumstance). Only works for secure settings of type string (not file).
     *
     * @param source               A {@code Settings} object with its {@code SecureSettings} open/available.
     * @param securePluginSettings The list of settings to copy.
     * @return A copy of the {@code SecureSettings} of the passed in {@code Settings} argument.
     */
    private static SecureSettings extractSecureSettings(Settings source, List<Setting<?>> securePluginSettings)
        throws GeneralSecurityException {
        // get the secure settings out
        final SecureSettings sourceSecureSettings = Settings.builder().put(source, true).getSecureSettings();
        // filter and cache them...
        final Map<String, Tuple<SecureString, byte[]>> cache = new HashMap<>();
        if (sourceSecureSettings != null && securePluginSettings != null) {
            for (final String settingKey : sourceSecureSettings.getSettingNames()) {
                for (final Setting<?> secureSetting : securePluginSettings) {
                    if (secureSetting.match(settingKey)) {
                        cache.put(
                            settingKey,
                            new Tuple<>(sourceSecureSettings.getString(settingKey), sourceSecureSettings.getSHA256Digest(settingKey))
                        );
                    }
                }
            }
        }
        return new SecureSettings() {
            @Override
            public boolean isLoaded() {
                return true;
            }

            @Override
            public SecureString getString(String setting) {
                return cache.get(setting).v1();
            }

            @Override
            public Set<String> getSettingNames() {
                return cache.keySet();
            }

            @Override
            public InputStream getFile(String setting) {
                throw new IllegalStateException("A NotificationService setting cannot be File.");
            }

            @Override
            public byte[] getSHA256Digest(String setting) {
                return cache.get(setting).v2();
            }

            @Override
            public void close() throws IOException {}

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                throw new IllegalStateException("Unsupported operation");
            }
        };
    }
}
