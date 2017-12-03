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

package org.elasticsearch.repositories.s3;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

/**
 * A container for settings used to create an S3 client.
 */
class S3ClientSettings {

    // prefix for s3 client settings
    private static final String PREFIX = "s3.client.";

    /** The access key (ie login id) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> ACCESS_KEY_SETTING = Setting.affixKeySetting(PREFIX, "access_key",
        key -> SecureSetting.secureString(key, null));

    /** The secret key (ie password) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> SECRET_KEY_SETTING = Setting.affixKeySetting(PREFIX, "secret_key",
        key -> SecureSetting.secureString(key, null));

    /** An override for the s3 endpoint to connect to. */
    static final Setting.AffixSetting<String> ENDPOINT_SETTING = Setting.affixKeySetting(PREFIX, "endpoint",
        key -> new Setting<>(key, "", s -> s.toLowerCase(Locale.ROOT), Property.NodeScope));

    /** The protocol to use to connect to s3. */
    static final Setting.AffixSetting<Protocol> PROTOCOL_SETTING = Setting.affixKeySetting(PREFIX, "protocol",
        key -> new Setting<>(key, "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope));

    /** The host name of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(PREFIX, "proxy.host",
        key -> Setting.simpleString(key, Property.NodeScope));

    /** The port of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(PREFIX, "proxy.port",
        key -> Setting.intSetting(key, 80, 0, 1<<16, Property.NodeScope));

    /** The username of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<SecureString> PROXY_USERNAME_SETTING = Setting.affixKeySetting(PREFIX, "proxy.username",
        key -> SecureSetting.secureString(key, null));

    /** The password of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<SecureString> PROXY_PASSWORD_SETTING = Setting.affixKeySetting(PREFIX, "proxy.password",
        key -> SecureSetting.secureString(key, null));

    /** The socket timeout for connecting to s3. */
    static final Setting.AffixSetting<TimeValue> READ_TIMEOUT_SETTING = Setting.affixKeySetting(PREFIX, "read_timeout",
        key -> Setting.timeSetting(key, TimeValue.timeValueMillis(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT), Property.NodeScope));

    /** The number of retries to use when an s3 request fails. */
    static final Setting.AffixSetting<Integer> MAX_RETRIES_SETTING = Setting.affixKeySetting(PREFIX, "max_retries",
        key -> Setting.intSetting(key, ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry(), 0, Property.NodeScope));

    /** Whether retries should be throttled (ie use backoff). */
    static final Setting.AffixSetting<Boolean> USE_THROTTLE_RETRIES_SETTING = Setting.affixKeySetting(PREFIX, "use_throttle_retries",
        key -> Setting.boolSetting(key, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, Property.NodeScope));

    /** Credentials to authenticate with s3. */
    final BasicAWSCredentials credentials;

    /** The s3 endpoint the client should talk to, or empty string to use the default. */
    final String endpoint;

    /** The protocol to use to talk to s3. Defaults to https. */
    final Protocol protocol;

    /** An optional proxy host that requests to s3 should be made through. */
    final String proxyHost;

    /** The port number the proxy host should be connected on. */
    final int proxyPort;

    // these should be "secure" yet the api for the s3 client only takes String, so storing them
    // as SecureString here won't really help with anything
    /** An optional username for the proxy host, for basic authentication. */
    final String proxyUsername;

    /** An optional password for the proxy host, for basic authentication. */
    final String proxyPassword;

    /** The read timeout for the s3 client. */
    final int readTimeoutMillis;

    /** The number of retries to use for the s3 client. */
    final int maxRetries;

    /** Whether the s3 client should use an exponential backoff retry policy. */
    final boolean throttleRetries;

    private S3ClientSettings(BasicAWSCredentials credentials, String endpoint, Protocol protocol,
                             String proxyHost, int proxyPort, String proxyUsername, String proxyPassword,
                             int readTimeoutMillis, int maxRetries, boolean throttleRetries) {
        this.credentials = credentials;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.readTimeoutMillis = readTimeoutMillis;
        this.maxRetries = maxRetries;
        this.throttleRetries = throttleRetries;
    }

    /**
     * Load all client settings from the given settings.
     *
     * Note this will always at least return a client named "default".
     */
    static Map<String, S3ClientSettings> load(Settings settings) {
        Set<String> clientNames = settings.getGroups(PREFIX).keySet();
        Map<String, S3ClientSettings> clients = new HashMap<>();
        for (String clientName : clientNames) {
            clients.put(clientName, getClientSettings(settings, clientName));
        }
        if (clients.containsKey("default") == false) {
            // this won't find any settings under the default client,
            // but it will pull all the fallback static settings
            clients.put("default", getClientSettings(settings, "default"));
        }
        return Collections.unmodifiableMap(clients);
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    static S3ClientSettings getClientSettings(Settings settings, String clientName) {
        try (SecureString accessKey = getConfigValue(settings, clientName, ACCESS_KEY_SETTING);
             SecureString secretKey = getConfigValue(settings, clientName, SECRET_KEY_SETTING);
             SecureString proxyUsername = getConfigValue(settings, clientName, PROXY_USERNAME_SETTING);
             SecureString proxyPassword = getConfigValue(settings, clientName, PROXY_PASSWORD_SETTING)) {
            BasicAWSCredentials credentials = null;
            if (accessKey.length() != 0) {
                if (secretKey.length() != 0) {
                    credentials = new BasicAWSCredentials(accessKey.toString(), secretKey.toString());
                } else {
                    throw new IllegalArgumentException("Missing secret key for s3 client [" + clientName + "]");
                }
            } else if (secretKey.length() != 0) {
                throw new IllegalArgumentException("Missing access key for s3 client [" + clientName + "]");
            }
            return new S3ClientSettings(
                credentials,
                getConfigValue(settings, clientName, ENDPOINT_SETTING),
                getConfigValue(settings, clientName, PROTOCOL_SETTING),
                getConfigValue(settings, clientName, PROXY_HOST_SETTING),
                getConfigValue(settings, clientName, PROXY_PORT_SETTING),
                proxyUsername.toString(),
                proxyPassword.toString(),
                (int)getConfigValue(settings, clientName, READ_TIMEOUT_SETTING).millis(),
                getConfigValue(settings, clientName, MAX_RETRIES_SETTING),
                getConfigValue(settings, clientName, USE_THROTTLE_RETRIES_SETTING)
            );
        }
    }

    private static <T> T getConfigValue(Settings settings, String clientName,
                                        Setting.AffixSetting<T> clientSetting) {
        Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }

}
