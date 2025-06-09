/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.ServiceAccountCredentials;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.settings.Setting.timeSetting;

/**
 * Container for Google Cloud Storage clients settings.
 */
public class GoogleCloudStorageClientSettings {

    private static final String PREFIX = "gcs.client.";

    /** A json Service Account file loaded from secure settings. */
    static final Setting.AffixSetting<InputStream> CREDENTIALS_FILE_SETTING = Setting.affixKeySetting(
        PREFIX,
        "credentials_file",
        key -> SecureSetting.secureFile(key, null)
    );

    /** An override for the Storage endpoint to connect to. */
    static final Setting.AffixSetting<String> ENDPOINT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "endpoint",
        key -> Setting.simpleString(key, Setting.Property.NodeScope)
    );

    /** An override for the Google Project ID. */
    static final Setting.AffixSetting<String> PROJECT_ID_SETTING = Setting.affixKeySetting(
        PREFIX,
        "project_id",
        key -> Setting.simpleString(key, Setting.Property.NodeScope)
    );

    /** An override for the Token Server URI in the oauth flow. */
    static final Setting.AffixSetting<URI> TOKEN_URI_SETTING = Setting.affixKeySetting(
        PREFIX,
        "token_uri",
        key -> new Setting<>(key, "", URI::create, Setting.Property.NodeScope)
    );

    /**
     * The timeout to establish a connection. A value of {@code -1} corresponds to an infinite timeout. A value of {@code 0}
     * corresponds to the default timeout of the Google Cloud Storage Java Library.
     */
    static final Setting.AffixSetting<TimeValue> CONNECT_TIMEOUT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "connect_timeout",
        key -> timeSetting(key, TimeValue.ZERO, TimeValue.MINUS_ONE, Setting.Property.NodeScope)
    );

    /**
     * The timeout to read data from an established connection. A value of {@code -1} corresponds to an infinite timeout. A value of
     * {@code 0} corresponds to the default timeout of the Google Cloud Storage Java Library.
     */
    static final Setting.AffixSetting<TimeValue> READ_TIMEOUT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "read_timeout",
        key -> timeSetting(key, TimeValue.ZERO, TimeValue.MINUS_ONE, Setting.Property.NodeScope)
    );

    /** Name used by the client when it uses the Google Cloud JSON API. */
    static final Setting.AffixSetting<String> APPLICATION_NAME_SETTING = Setting.affixKeySetting(
        PREFIX,
        "application_name",
        key -> new Setting<>(key, "repository-gcs", Function.identity(), Setting.Property.NodeScope, Setting.Property.DeprecatedWarning)
    );

    /** The type of the proxy to connect to the GCS through. Can be DIRECT (aka no proxy), HTTP or SOCKS */
    public static final Setting.AffixSetting<Proxy.Type> PROXY_TYPE_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.type",
        (key) -> new Setting<>(key, "DIRECT", s -> Proxy.Type.valueOf(s.toUpperCase(Locale.ROOT)), Setting.Property.NodeScope)
    );

    /** The host name of a proxy to connect to the GCS through. */
    static final Setting.AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.host",
        (key) -> Setting.simpleString(key, Setting.Property.NodeScope),
        () -> PROXY_TYPE_SETTING
    );

    /** The port of a proxy to connect to the GCS through. */
    static final Setting.AffixSetting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.port",
        (key) -> Setting.intSetting(key, 0, 0, 65535, Setting.Property.NodeScope),
        () -> PROXY_HOST_SETTING
    );

    /** The credentials used by the client to connect to the Storage endpoint. */
    private final ServiceAccountCredentials credential;

    /** The Storage endpoint URL the client should talk to. Null value sets the default. */
    private final String endpoint;

    /** The Google project ID overriding the default way to infer it. Null value sets the default. */
    private final String projectId;

    /** The timeout to establish a connection */
    private final TimeValue connectTimeout;

    /** The timeout to read data from an established connection */
    private final TimeValue readTimeout;

    /** The Storage client application name */
    private final String applicationName;

    /** The token server URI. This leases access tokens in the oauth flow. */
    private final URI tokenUri;

    @Nullable
    private final Proxy proxy;

    GoogleCloudStorageClientSettings(
        final ServiceAccountCredentials credential,
        final String endpoint,
        final String projectId,
        final TimeValue connectTimeout,
        final TimeValue readTimeout,
        final String applicationName,
        final URI tokenUri,
        final Proxy proxy
    ) {
        this.credential = credential;
        this.endpoint = endpoint;
        this.projectId = projectId;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.applicationName = applicationName;
        this.tokenUri = tokenUri;
        this.proxy = proxy;
    }

    public ServiceAccountCredentials getCredential() {
        return credential;
    }

    public String getHost() {
        return endpoint;
    }

    public String getProjectId() {
        return Strings.hasLength(projectId) ? projectId : (credential != null ? credential.getProjectId() : null);
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

    public URI getTokenUri() {
        return tokenUri;
    }

    @Nullable
    public Proxy getProxy() {
        return proxy;
    }

    public static Map<String, GoogleCloudStorageClientSettings> load(final Settings settings) {
        final Map<String, GoogleCloudStorageClientSettings> clients = new HashMap<>();
        for (final String clientName : settings.getGroups(PREFIX).keySet()) {
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
        Proxy.Type proxyType = getConfigValue(settings, clientName, PROXY_TYPE_SETTING);
        String proxyHost = getConfigValue(settings, clientName, PROXY_HOST_SETTING);
        Integer proxyPort = getConfigValue(settings, clientName, PROXY_PORT_SETTING);
        Proxy proxy;
        try {
            proxy = proxyType.equals(Proxy.Type.DIRECT)
                ? null
                : new Proxy(proxyType, new InetSocketAddress(InetAddress.getByName(proxyHost), proxyPort));
        } catch (UnknownHostException e) {
            throw new SettingsException("GCS proxy host is unknown.", e);
        }
        return new GoogleCloudStorageClientSettings(
            loadCredential(settings, clientName, proxy),
            getConfigValue(settings, clientName, ENDPOINT_SETTING),
            getConfigValue(settings, clientName, PROJECT_ID_SETTING),
            getConfigValue(settings, clientName, CONNECT_TIMEOUT_SETTING),
            getConfigValue(settings, clientName, READ_TIMEOUT_SETTING),
            getConfigValue(settings, clientName, APPLICATION_NAME_SETTING),
            getConfigValue(settings, clientName, TOKEN_URI_SETTING),
            proxy
        );
    }

    /**
     * Loads the service account file corresponding to a given client name. If no
     * file is defined for the client, a {@code null} credential is returned.
     *
     * @param settings
     *            the {@link Settings}
     * @param clientName
     *            the client name
     *
     * @return the {@link ServiceAccountCredentials} to use for the given client,
     *         {@code null} if no service account is defined.
     */
    static ServiceAccountCredentials loadCredential(final Settings settings, final String clientName, @Nullable Proxy proxy) {
        final var credentialsFileSetting = CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName);
        try {
            if (credentialsFileSetting.exists(settings) == false) {
                // explicitly returning null here so that the default credential
                // can be loaded later when creating the Storage client
                return null;
            }
            try (InputStream credStream = credentialsFileSetting.get(settings)) {
                final Collection<String> scopes = Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL);
                NetHttpTransport netHttpTransport = new NetHttpTransport.Builder().setProxy(proxy).build();
                final ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(credStream, () -> netHttpTransport);
                if (credentials.createScopedRequired()) {
                    return (ServiceAccountCredentials) credentials.createScoped(scopes);
                }
                return credentials;
            }
        } catch (final Exception e) {
            throw new IllegalArgumentException("failed to load GCS client credentials from [" + credentialsFileSetting.getKey() + "]", e);
        }
    }

    private static <T> T getConfigValue(final Settings settings, final String clientName, final Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }
}
