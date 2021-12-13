/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.jwt;

import org.apache.http.HttpHost;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.ClaimSetting;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JwtRealmSettings {

    private JwtRealmSettings() {}

    public static final String TYPE = "jwt";

    public static final List<String> SUPPORTED_SIGNATURE_ALGORITHMS = List.of(
        "HS256",
        "HS384",
        "HS512",
        "RS256",
        "RS384",
        "RS512",
        "ES256",
        "ES384",
        "ES512",
        "PS256",
        "PS384",
        "PS512"
    );

    // HTTP settings

    private static final TimeValue HTTP_DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(5);
    public static final Setting.AffixSetting<TimeValue> HTTP_CONNECT_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.connect_timeout",
        key -> Setting.timeSetting(key, HTTP_DEFAULT_TIMEOUT, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> HTTP_CONNECTION_READ_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.connection_read_timeout",
        key -> Setting.timeSetting(key, HTTP_DEFAULT_TIMEOUT, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> HTTP_SOCKET_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.socket_timeout",
        key -> Setting.timeSetting(key, HTTP_DEFAULT_TIMEOUT, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Integer> HTTP_MAX_CONNECTIONS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.max_connections",
        key -> Setting.intSetting(key, 200, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Integer> HTTP_MAX_ENDPOINT_CONNECTIONS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.max_endpoint_connections",
        key -> Setting.intSetting(key, 200, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<String> HTTP_PROXY_HOST = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.proxy.host",
        key -> Setting.simpleString(key, new Setting.Validator<String>() {
            @Override
            public void validate(String value) {
                // There is no point in validating the hostname in itself without the scheme and port
            }

            @Override
            public void validate(String value, Map<Setting<?>, Object> settings) {
                final String namespace = HTTP_PROXY_HOST.getNamespace(HTTP_PROXY_HOST.getConcreteSetting(key));
                final Setting<Integer> portSetting = HTTP_PROXY_PORT.getConcreteSettingForNamespace(namespace);
                final Integer port = (Integer) settings.get(portSetting);
                final Setting<String> schemeSetting = HTTP_PROXY_SCHEME.getConcreteSettingForNamespace(namespace);
                final String scheme = (String) settings.get(schemeSetting);
                try {
                    new HttpHost(value, port, scheme);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                        "HTTP host for hostname ["
                            + value
                            + "] (from ["
                            + key
                            + "]),"
                            + " port ["
                            + port
                            + "] (from ["
                            + portSetting.getKey()
                            + "]) and "
                            + "scheme ["
                            + scheme
                            + "] (from (["
                            + schemeSetting.getKey()
                            + "]) is invalid"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final String namespace = HTTP_PROXY_HOST.getNamespace(HTTP_PROXY_HOST.getConcreteSetting(key));
                final List<Setting<?>> settings = List.of(
                    HTTP_PROXY_PORT.getConcreteSettingForNamespace(namespace),
                    HTTP_PROXY_SCHEME.getConcreteSettingForNamespace(namespace)
                );
                return settings.iterator();
            }
        }, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Integer> HTTP_PROXY_PORT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.proxy.port",
        key -> Setting.intSetting(key, 80, 1, 65535, Setting.Property.NodeScope),
        () -> HTTP_PROXY_HOST
    );
    public static final Setting.AffixSetting<String> HTTP_PROXY_SCHEME = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.proxy.scheme",
        key -> Setting.simpleString(key, "http", value -> {
            if (value.equals("http") == false && value.equals("https") == false) {
                throw new IllegalArgumentException("Invalid value [" + value + "] for [" + key + "]. Only `http` or `https` are allowed.");
            }
        }, Setting.Property.NodeScope)
    );

    // JWT settings

    public static final Setting.AffixSetting<String> ISSUER = RealmSettings.simpleString(TYPE, "issuer", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> JWKSET_PATH = RealmSettings.simpleString(
        TYPE,
        "jwkset_path",
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<TimeValue> ALLOWED_CLOCK_SKEW = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_clock_skew",
        key -> Setting.timeSetting(key, TimeValue.timeValueSeconds(60), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Boolean> POPULATE_USER_METADATA = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "populate_user_metadata",
        key -> Setting.boolSetting(key, true, Setting.Property.NodeScope)
    );
    public static final ClaimSetting PRINCIPAL_CLAIM = new ClaimSetting(TYPE, "principal");
    public static final ClaimSetting GROUPS_CLAIM = new ClaimSetting(TYPE, "groups");

    // All settings

    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> set = Sets.newHashSet(
            HTTP_CONNECT_TIMEOUT,
            HTTP_CONNECTION_READ_TIMEOUT,
            HTTP_SOCKET_TIMEOUT,
            HTTP_MAX_CONNECTIONS,
            HTTP_MAX_ENDPOINT_CONNECTIONS,
            HTTP_PROXY_HOST,
            HTTP_PROXY_PORT,
            HTTP_PROXY_SCHEME,
            ISSUER,
            JWKSET_PATH,
            ALLOWED_CLOCK_SKEW,
            POPULATE_USER_METADATA
        );
        set.addAll(PRINCIPAL_CLAIM.settings());
        set.addAll(GROUPS_CLAIM.settings());
        set.addAll(SSLConfigurationSettings.getRealmSettings(TYPE));
        set.addAll(RealmSettings.getStandardSettings(TYPE));
        set.addAll(DelegatedAuthorizationSettings.getSettings(TYPE));
        return set;
    }
}
