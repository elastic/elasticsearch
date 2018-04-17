/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.gcs;

import com.google.auth.oauth2.ServiceAccountCredentials;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.settings.Setting.timeSetting;

/**
 * Container for Google Cloud Storage clients settings.
 */
public class GoogleCloudStorageClientSettings {

    private static final String PREFIX = "gcs.client.";

    /** A json Service Account file loaded from secure settings. */
    static final Setting.AffixSetting<InputStream> CREDENTIALS_FILE_SETTING = Setting.affixKeySetting(PREFIX, "credentials_file",
            key -> SecureSetting.secureFile(key, null));

    /**
     * An override for the Storage endpoint to connect to. Deprecated, use host
     * setting.
     */
    static final Setting.AffixSetting<String> ENDPOINT_SETTING = Setting.affixKeySetting(PREFIX, "endpoint",
            key -> Setting.simpleString(key, Setting.Property.NodeScope, Setting.Property.Deprecated));

    /** An override for the Storage host name to connect to. */
    static final Setting.AffixSetting<String> HOST_SETTING = Setting.affixKeySetting(PREFIX, "host",
            key -> Setting.simpleString(key,
                    ENDPOINT_SETTING.getConcreteSetting(key.substring(0, key.length() - "host".length()) + "endpoint"),
                    Setting.Property.NodeScope));

    /** An override for the Google Project ID. */
    static final Setting.AffixSetting<String> PROJECT_ID_SETTING = Setting.affixKeySetting(PREFIX, "project_id",
            key -> Setting.simpleString(key, Setting.Property.NodeScope));

    /**
     * The timeout to establish a connection. A value of {@code -1} corresponds to an infinite timeout. A value of {@code 0}
     * corresponds to the default timeout of the Google Cloud Storage Java Library.
     */
    static final Setting.AffixSetting<TimeValue> CONNECT_TIMEOUT_SETTING = Setting.affixKeySetting(PREFIX, "connect_timeout",
        key -> timeSetting(key, TimeValue.ZERO, TimeValue.MINUS_ONE, Setting.Property.NodeScope));

    /**
     * The timeout to read data from an established connection. A value of {@code -1} corresponds to an infinite timeout. A value of
     * {@code 0} corresponds to the default timeout of the Google Cloud Storage Java Library.
     */
    static final Setting.AffixSetting<TimeValue> READ_TIMEOUT_SETTING = Setting.affixKeySetting(PREFIX, "read_timeout",
        key -> timeSetting(key, TimeValue.ZERO, TimeValue.MINUS_ONE, Setting.Property.NodeScope));

    /** Name used by the client when it uses the Google Cloud JSON API. **/
    static final Setting.AffixSetting<String> APPLICATION_NAME_SETTING = Setting.affixKeySetting(PREFIX, "application_name",
            key -> new Setting<>(key, "elasticsearch-repository-gcs", Function.identity(), Setting.Property.NodeScope,
                    Setting.Property.Deprecated));

    /** The credentials used by the client to connect to the Storage endpoint **/
    private final ServiceAccountCredentials credential;

    /**
     * The Storage root URL (hostname) the client should talk to, or null string to
     * use the default.
     **/
    private final String host;

    /**
     * The Google project ID overriding the default way to infer it. Null value sets
     * the default.
     **/
    private final String projectId;

    /** The timeout to establish a connection **/
    private final TimeValue connectTimeout;

    /** The timeout to read data from an established connection **/
    private final TimeValue readTimeout;

    /** The Storage client application name **/
    private final String applicationName;

    GoogleCloudStorageClientSettings(final ServiceAccountCredentials credential,
            final String host, final String projectId,
                                     final TimeValue connectTimeout,
                                     final TimeValue readTimeout,
                                     final String applicationName) {
        this.credential = credential;
        this.host = host;
        this.projectId = projectId;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.applicationName = applicationName;
    }

    public ServiceAccountCredentials getCredential() {
        return credential;
    }

    public String getHost() {
        return host;
    }

    public String getProjectId() {
        return projectId;
    }

    public TimeValue getConnectTimeout() {
        return connectTimeout;
    }

    public TimeValue getReadTimeout() {
        return readTimeout;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public static Map<String, GoogleCloudStorageClientSettings> load(final Settings settings) {
        final Map<String, GoogleCloudStorageClientSettings> clients = new HashMap<>();
        for (final String clientName: settings.getGroups(PREFIX).keySet()) {
            clients.put(clientName, getClientSettings(settings, clientName));
        }
        if (clients.containsKey("default") == false) {
            // this won't find any settings under the default client,
            // but it will pull all the fallback static settings
            clients.put("default", getClientSettings(settings, "default"));
        }
        return Collections.unmodifiableMap(clients);
    }

    static GoogleCloudStorageClientSettings getClientSettings(final Settings settings, final String clientName) {
        return new GoogleCloudStorageClientSettings(
            loadCredential(settings, clientName),
                getConfigValue(settings, clientName, HOST_SETTING), getConfigValue(settings, clientName, PROJECT_ID_SETTING),
            getConfigValue(settings, clientName, CONNECT_TIMEOUT_SETTING),
            getConfigValue(settings, clientName, READ_TIMEOUT_SETTING),
            getConfigValue(settings, clientName, APPLICATION_NAME_SETTING)
        );
    }

    /**
     * Loads the service account file corresponding to a given client name. If no file is defined for the client,
     * a {@code null} credential is returned.
     *
     * @param settings the {@link Settings}
     * @param clientName the client name
     *
     * @return the {@link GoogleCredential} to use for the given client, {@code null} if no service account is defined.
     */
    static ServiceAccountCredentials loadCredential(final Settings settings, final String clientName) {
        try {
            if (CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).exists(settings) == false) {
                // explicitly returning null here so that the default credential
                // can be loaded later when creating the Storage client
                return null;
            }
            try (InputStream credStream = CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).get(settings)) {
                return ServiceAccountCredentials.fromStream(credStream);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static <T> T getConfigValue(final Settings settings, final String clientName, final Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }
}
