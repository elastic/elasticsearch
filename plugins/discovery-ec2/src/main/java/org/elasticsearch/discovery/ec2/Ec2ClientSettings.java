/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

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

    /** The scheme to use for the proxy connection to ec2. Defaults to "http". */
    static final Setting<HttpScheme> PROXY_SCHEME_SETTING = new Setting<>(
        "discovery.ec2.proxy.scheme",
        "http",
        s -> HttpScheme.valueOf(s.toUpperCase(Locale.ROOT)),
        Property.NodeScope
    );

    /** An override for the ec2 endpoint to connect to. */
    static final Setting<String> ENDPOINT_SETTING = new Setting<>(
        "discovery.ec2.endpoint",
        "",
        s -> s.toLowerCase(Locale.ROOT),
        Property.NodeScope
    );

    /** Previously, the protocol to use to connect to ec2, but now has no effect */
    static final Setting<HttpScheme> PROTOCOL_SETTING = new Setting<>(
        "discovery.ec2.protocol",
        "https",
        s -> HttpScheme.valueOf(s.toUpperCase(Locale.ROOT)),
        Property.NodeScope,
        Property.DeprecatedWarning
    );

    /** The username of a proxy to connect to EC2 through. */
    static final Setting<SecureString> PROXY_USERNAME_SETTING = SecureSetting.secureString("discovery.ec2.proxy.username", null);

    /** The password of a proxy to connect to EC2 through. */
    static final Setting<SecureString> PROXY_PASSWORD_SETTING = SecureSetting.secureString("discovery.ec2.proxy.password", null);

    private static final TimeValue DEFAULT_READ_TIMEOUT = TimeValue.timeValueSeconds(50);

    /** The socket timeout for connecting to EC2. */
    static final Setting<TimeValue> READ_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.ec2.read_timeout",
        DEFAULT_READ_TIMEOUT,
        Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(Ec2ClientSettings.class);

    /** Credentials to authenticate with ec2. */
    final AwsCredentials credentials;

    /**
     * The ec2 endpoint the client should talk to, or empty string to use the
     * default.
     */
    final String endpoint;

    /** An optional proxy host that requests to ec2 should be made through. */
    final String proxyHost;

    /** The port number the proxy host should be connected on. */
    final int proxyPort;

    /** The scheme to use for the proxy connection to ec2 */
    final HttpScheme proxyScheme;

    // these should be "secure" yet the api for the ec2 client only takes String, so
    // storing them
    // as SecureString here won't really help with anything
    /** An optional username for the proxy host, for basic authentication. */
    final String proxyUsername;

    /** An optional password for the proxy host, for basic authentication. */
    final String proxyPassword;

    /** The read timeout for the ec2 client. */
    final int readTimeoutMillis;

    private Ec2ClientSettings(
        AwsCredentials credentials,
        String endpoint,
        String proxyHost,
        int proxyPort,
        HttpScheme proxyScheme,
        String proxyUsername,
        String proxyPassword,
        int readTimeoutMillis
    ) {
        this.credentials = credentials;
        this.endpoint = endpoint;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyScheme = proxyScheme;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    static AwsCredentials loadCredentials(Settings settings) {
        try (
            SecureString key = ACCESS_KEY_SETTING.get(settings);
            SecureString secret = SECRET_KEY_SETTING.get(settings);
            SecureString sessionToken = SESSION_TOKEN_SETTING.get(settings)
        ) {
            if (key.length() == 0 && secret.length() == 0) {
                if (sessionToken.length() > 0) {
                    throw new SettingsException(
                        "Setting [{}] is set but [{}] and [{}] are not",
                        SESSION_TOKEN_SETTING.getKey(),
                        ACCESS_KEY_SETTING.getKey(),
                        SECRET_KEY_SETTING.getKey()
                    );
                }

                logger.debug("Using either environment variables, system properties or instance profile credentials");
                return null;
            } else {
                if (key.length() == 0) {
                    throw new SettingsException(
                        "Setting [{}] is set but [{}] is not",
                        SECRET_KEY_SETTING.getKey(),
                        ACCESS_KEY_SETTING.getKey()
                    );
                }
                if (secret.length() == 0) {
                    throw new SettingsException(
                        "Setting [{}] is set but [{}] is not",
                        ACCESS_KEY_SETTING.getKey(),
                        SECRET_KEY_SETTING.getKey()
                    );
                }

                if (sessionToken.length() == 0) {
                    logger.debug("Using basic key/secret credentials");
                    return AwsBasicCredentials.create(key.toString(), secret.toString());
                } else {
                    logger.debug("Using session credentials");
                    return AwsSessionCredentials.create(key.toString(), secret.toString(), sessionToken.toString());
                }
            }
        }
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    static Ec2ClientSettings getClientSettings(Settings settings) {
        final AwsCredentials credentials = loadCredentials(settings);
        try (
            SecureString proxyUsername = PROXY_USERNAME_SETTING.get(settings);
            SecureString proxyPassword = PROXY_PASSWORD_SETTING.get(settings)
        ) {
            return new Ec2ClientSettings(
                credentials,
                ENDPOINT_SETTING.get(settings),
                PROXY_HOST_SETTING.get(settings),
                PROXY_PORT_SETTING.get(settings),
                PROXY_SCHEME_SETTING.get(settings),
                proxyUsername.toString(),
                proxyPassword.toString(),
                Math.toIntExact(READ_TIMEOUT_SETTING.get(settings).millis())
            );
        }
    }

}
