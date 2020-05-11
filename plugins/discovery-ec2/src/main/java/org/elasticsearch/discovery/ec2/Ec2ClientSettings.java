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

package org.elasticsearch.discovery.ec2;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;

/**
 * A container for settings used to create an EC2 client.
 */
final class Ec2ClientSettings {

    /** The access key (ie login id) for connecting to ec2. */
    static final Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.secureString("discovery.ec2.access_key", null);

    /** The secret key (ie password) for connecting to ec2. */
    static final Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.secureString("discovery.ec2.secret_key", null);

    /** The session token for connecting to ec2. */
    static final Setting<SecureString> SESSION_TOKEN_SETTING = SecureSetting.secureString("discovery.ec2.session_token", null);

    /** The host name of a proxy to connect to ec2 through. */
    static final Setting<String> PROXY_HOST_SETTING = Setting.simpleString("discovery.ec2.proxy.host", Property.NodeScope);

    /** The port of a proxy to connect to ec2 through. */
    static final Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("discovery.ec2.proxy.port", 80, 0, 1 << 16, Property.NodeScope);

    /** An override for the ec2 endpoint to connect to. */
    static final Setting<String> ENDPOINT_SETTING = new Setting<>("discovery.ec2.endpoint", "", s -> s.toLowerCase(Locale.ROOT),
            Property.NodeScope);

    /** The protocol to use to connect to to ec2. */
    static final Setting<Protocol> PROTOCOL_SETTING = new Setting<>("discovery.ec2.protocol", "https",
            s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope);

    /** The username of a proxy to connect to s3 through. */
    static final Setting<SecureString> PROXY_USERNAME_SETTING = SecureSetting.secureString("discovery.ec2.proxy.username", null);

    /** The password of a proxy to connect to s3 through. */
    static final Setting<SecureString> PROXY_PASSWORD_SETTING = SecureSetting.secureString("discovery.ec2.proxy.password", null);

    /** The socket timeout for connecting to s3. */
    static final Setting<TimeValue> READ_TIMEOUT_SETTING = Setting.timeSetting("discovery.ec2.read_timeout",
            TimeValue.timeValueMillis(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT), Property.NodeScope);

    private static final Logger logger = LogManager.getLogger(Ec2ClientSettings.class);

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    /** Credentials to authenticate with ec2. */
    final AWSCredentials credentials;

    /**
     * The ec2 endpoint the client should talk to, or empty string to use the
     * default.
     */
    final String endpoint;

    /** The protocol to use to talk to ec2. Defaults to https. */
    final Protocol protocol;

    /** An optional proxy host that requests to ec2 should be made through. */
    final String proxyHost;

    /** The port number the proxy host should be connected on. */
    final int proxyPort;

    // these should be "secure" yet the api for the ec2 client only takes String, so
    // storing them
    // as SecureString here won't really help with anything
    /** An optional username for the proxy host, for basic authentication. */
    final String proxyUsername;

    /** An optional password for the proxy host, for basic authentication. */
    final String proxyPassword;

    /** The read timeout for the ec2 client. */
    final int readTimeoutMillis;

    protected Ec2ClientSettings(AWSCredentials credentials, String endpoint, Protocol protocol, String proxyHost, int proxyPort,
            String proxyUsername, String proxyPassword, int readTimeoutMillis) {
        this.credentials = credentials;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    static AWSCredentials loadCredentials(Settings settings) {
        try (SecureString key = ACCESS_KEY_SETTING.get(settings);
             SecureString secret = SECRET_KEY_SETTING.get(settings);
             SecureString sessionToken = SESSION_TOKEN_SETTING.get(settings)) {
            if (key.length() == 0 && secret.length() == 0) {
                if (sessionToken.length() > 0) {
                    throw new SettingsException("Setting [{}] is set but [{}] and [{}] are not",
                        SESSION_TOKEN_SETTING.getKey(), ACCESS_KEY_SETTING.getKey(), SECRET_KEY_SETTING.getKey());
                }

                logger.debug("Using either environment variables, system properties or instance profile credentials");
                return null;
            } else {
                if (key.length() == 0) {
                    deprecationLogger.deprecatedAndMaybeLog("ec2_invalid_settings",
                        "Setting [{}] is set but [{}] is not, which will be unsupported in future",
                        SECRET_KEY_SETTING.getKey(), ACCESS_KEY_SETTING.getKey());
                }
                if (secret.length() == 0) {
                    deprecationLogger.deprecatedAndMaybeLog("ec2_invalid_settings",
                       "Setting [{}] is set but [{}] is not, which will be unsupported in future",
                        ACCESS_KEY_SETTING.getKey(), SECRET_KEY_SETTING.getKey());
                }

                final AWSCredentials credentials;
                if (sessionToken.length() == 0) {
                    logger.debug("Using basic key/secret credentials");
                    credentials = new BasicAWSCredentials(key.toString(), secret.toString());
                } else {
                    logger.debug("Using basic session credentials");
                    credentials = new BasicSessionCredentials(key.toString(), secret.toString(), sessionToken.toString());
                }
                return credentials;
            }
        }
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    static Ec2ClientSettings getClientSettings(Settings settings) {
        final AWSCredentials credentials = loadCredentials(settings);
        try (SecureString proxyUsername = PROXY_USERNAME_SETTING.get(settings);
             SecureString proxyPassword = PROXY_PASSWORD_SETTING.get(settings)) {
            return new Ec2ClientSettings(
                credentials,
                ENDPOINT_SETTING.get(settings),
                PROTOCOL_SETTING.get(settings),
                PROXY_HOST_SETTING.get(settings),
                PROXY_PORT_SETTING.get(settings),
                proxyUsername.toString(),
                proxyPassword.toString(),
                (int)READ_TIMEOUT_SETTING.get(settings).millis());
        }
    }

}
