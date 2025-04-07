/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.ingest.EnterpriseGeoIpTask.ENTERPRISE_GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor.ENABLED_SETTING;
import static org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING;

public class EnterpriseGeoIpDownloaderTaskExecutor extends PersistentTasksExecutor<EnterpriseGeoIpTaskParams>
    implements
        ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(EnterpriseGeoIpDownloader.class);

    static final String MAXMIND_SETTINGS_PREFIX = "ingest.geoip.downloader.maxmind.";

    static final String IPINFO_SETTINGS_PREFIX = "ingest.ip_location.downloader.ipinfo.";

    public static final Setting<SecureString> MAXMIND_LICENSE_KEY_SETTING = SecureSetting.secureString(
        MAXMIND_SETTINGS_PREFIX + "license_key",
        null
    );

    public static final Setting<SecureString> IPINFO_TOKEN_SETTING = SecureSetting.secureString(IPINFO_SETTINGS_PREFIX + "token", null);

    private final Client client;
    private final HttpClient httpClient;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Settings settings;
    private volatile TimeValue pollInterval;
    private final AtomicReference<EnterpriseGeoIpDownloader> currentTask = new AtomicReference<>();

    private volatile SecureSettings cachedSecureSettings;

    EnterpriseGeoIpDownloaderTaskExecutor(Client client, HttpClient httpClient, ClusterService clusterService, ThreadPool threadPool) {
        super(ENTERPRISE_GEOIP_DOWNLOADER, threadPool.generic());
        this.client = new OriginSettingClient(client, IngestService.INGEST_ORIGIN);
        this.httpClient = httpClient;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.settings = clusterService.getSettings();
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);

        // do an initial load using the node settings
        reload(clusterService.getSettings());
    }

    /**
     * This method completes the initialization of the EnterpriseGeoIpDownloaderTaskExecutor by registering several listeners.
     */
    public void init() {
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::setPollInterval);
    }

    private void setPollInterval(TimeValue pollInterval) {
        if (Objects.equals(this.pollInterval, pollInterval) == false) {
            this.pollInterval = pollInterval;
            EnterpriseGeoIpDownloader currentDownloader = getCurrentTask();
            if (currentDownloader != null) {
                currentDownloader.requestReschedule();
            }
        }
    }

    private char[] getSecureToken(final String type) {
        char[] token = null;
        if (type.equals("maxmind")) {
            if (cachedSecureSettings.getSettingNames().contains(MAXMIND_LICENSE_KEY_SETTING.getKey())) {
                token = cachedSecureSettings.getString(MAXMIND_LICENSE_KEY_SETTING.getKey()).getChars();
            }
        } else if (type.equals("ipinfo")) {
            if (cachedSecureSettings.getSettingNames().contains(IPINFO_TOKEN_SETTING.getKey())) {
                token = cachedSecureSettings.getString(IPINFO_TOKEN_SETTING.getKey()).getChars();
            }
        }
        return token;
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
            this::getSecureToken
        );
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, EnterpriseGeoIpTaskParams params, PersistentTaskState state) {
        EnterpriseGeoIpDownloader downloader = (EnterpriseGeoIpDownloader) task;
        EnterpriseGeoIpTaskState geoIpTaskState = (state == null) ? EnterpriseGeoIpTaskState.EMPTY : (EnterpriseGeoIpTaskState) state;
        downloader.setState(geoIpTaskState);
        currentTask.set(downloader);
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
            boolean hasGeoIpMetadataChanges = event.metadataChanged()
                && event.changedCustomProjectMetadataSet().contains(IngestGeoIpMetadata.TYPE);
            if (hasGeoIpMetadataChanges) {
                currentDownloader.requestReschedule(); // watching the cluster changed events to kick the thing off if it's not running
            }
        }
    }

    public synchronized void reload(Settings settings) {
        // `SecureSettings` are available here! cache them as they will be needed
        // whenever dynamic cluster settings change and we have to rebuild the accounts
        try {
            this.cachedSecureSettings = extractSecureSettings(settings, List.of(MAXMIND_LICENSE_KEY_SETTING, IPINFO_TOKEN_SETTING));
        } catch (GeneralSecurityException e) {
            // rethrow as a runtime exception, there's logging higher up the call chain around ReloadablePlugin
            throw new ElasticsearchException("Exception while reloading enterprise geoip download task executor", e);
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
        final Map<String, SecureSettingValue> innerMap = new HashMap<>();
        if (sourceSecureSettings != null && securePluginSettings != null) {
            for (final String settingKey : sourceSecureSettings.getSettingNames()) {
                for (final Setting<?> secureSetting : securePluginSettings) {
                    if (secureSetting.match(settingKey)) {
                        innerMap.put(
                            settingKey,
                            new SecureSettingValue(
                                sourceSecureSettings.getString(settingKey),
                                sourceSecureSettings.getSHA256Digest(settingKey)
                            )
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
                return innerMap.get(setting).value();
            }

            @Override
            public Set<String> getSettingNames() {
                return innerMap.keySet();
            }

            @Override
            public InputStream getFile(String setting) {
                throw new UnsupportedOperationException("A cached SecureSetting cannot be a file");
            }

            @Override
            public byte[] getSHA256Digest(String setting) {
                return innerMap.get(setting).sha256Digest();
            }

            @Override
            public void close() throws IOException {}

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                throw new UnsupportedOperationException("A cached SecureSetting cannot be serialized");
            }
        };
    }

    /**
     * A single-purpose record for the internal implementation of extractSecureSettings
     */
    private record SecureSettingValue(SecureString value, byte[] sha256Digest) {}
}
