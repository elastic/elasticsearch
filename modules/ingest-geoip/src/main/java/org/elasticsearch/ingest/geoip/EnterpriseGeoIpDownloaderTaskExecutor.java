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
import org.elasticsearch.common.settings.InMemoryClonedSecureSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.NotMultiProjectCapable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.EnterpriseGeoIpTask.EnterpriseGeoIpTaskParams;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.ingest.EnterpriseGeoIpTask.ENTERPRISE_GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor.ENABLED_SETTING;
import static org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING;

@NotMultiProjectCapable(
    description = "Enterprise GeoIP not available in serverless, we should review this class for MP again after serverless is enabled"
)
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
                currentDownloader.restartPeriodicRun();
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
            downloader.restartPeriodicRun();
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
                // watching the cluster changed events to kick the thing off if it's not running
                currentDownloader.requestRunOnDemand();
            }
        }
    }

    public synchronized void reload(Settings settings) {
        // `SecureSettings` are available here! cache them as they will be needed
        // whenever dynamic cluster settings change and we have to rebuild the accounts
        try {
            this.cachedSecureSettings = InMemoryClonedSecureSettings.cloneSecureSettings(
                settings,
                List.of(MAXMIND_LICENSE_KEY_SETTING, IPINFO_TOKEN_SETTING)
            );
        } catch (GeneralSecurityException e) {
            // rethrow as a runtime exception, there's logging higher up the call chain around ReloadablePlugin
            throw new ElasticsearchException("Exception while reloading enterprise geoip download task executor", e);
        }
    }
}
