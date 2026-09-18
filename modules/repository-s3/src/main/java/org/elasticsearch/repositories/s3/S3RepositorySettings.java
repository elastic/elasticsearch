/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import org.elasticsearch.common.settings.Setting;

import java.util.ArrayList;
import java.util.List;

final class S3RepositorySettings {

    static final List<Setting<?>> SETTINGS = List.of(
        // named s3 client configuration settings
        S3ClientSettings.ACCESS_KEY_SETTING,
        S3ClientSettings.SECRET_KEY_SETTING,
        S3ClientSettings.SESSION_TOKEN_SETTING,
        S3ClientSettings.ENDPOINT_SETTING,
        S3ClientSettings.PROTOCOL_SETTING,
        S3ClientSettings.PROXY_HOST_SETTING,
        S3ClientSettings.PROXY_PORT_SETTING,
        S3ClientSettings.PROXY_SCHEME_SETTING,
        S3ClientSettings.PROXY_USERNAME_SETTING,
        S3ClientSettings.PROXY_PASSWORD_SETTING,
        S3ClientSettings.READ_TIMEOUT_SETTING,
        S3ClientSettings.MAX_CONNECTIONS_SETTING,
        S3ClientSettings.MAX_RETRIES_SETTING,
        S3ClientSettings.API_CALL_TIMEOUT_SETTING,
        S3ClientSettings.UNUSED_USE_THROTTLE_RETRIES_SETTING,
        S3ClientSettings.USE_PATH_STYLE_ACCESS,
        S3ClientSettings.DISABLE_CHUNKED_ENCODING,
        S3ClientSettings.UNUSED_SIGNER_OVERRIDE,
        S3ClientSettings.ADD_PURPOSE_CUSTOM_QUERY_PARAMETER,
        S3ClientSettings.REGION,
        S3ClientSettings.CONNECTION_MAX_IDLE_TIME_SETTING,
        S3ClientSettings.MAX_COPY_SIZE_BEFORE_MULTIPART,
        S3Service.REPOSITORY_S3_CAS_TTL_SETTING,
        S3Service.REPOSITORY_S3_CAS_ANTI_CONTENTION_DELAY_SETTING,
        S3Repository.ACCESS_KEY_SETTING,
        S3Repository.SECRET_KEY_SETTING,
        S3ClientSettings.S3_TENACIOUS_RETRIES_ENABLED_SETTING,
        S3ClientSettings.ALWAYS_SIGN_REQUESTS
    );

    static final List<Setting.AffixSetting<?>> DEPRECATED_CLIENT_SETTINGS = deprecatedClientSettings();

    private S3RepositorySettings() {}

    private static List<Setting.AffixSetting<?>> deprecatedClientSettings() {
        final List<Setting.AffixSetting<?>> deprecatedClientSettings = new ArrayList<>();
        for (Setting<?> setting : SETTINGS) {
            if (setting instanceof Setting.AffixSetting<?> affixSetting && setting.getProperties().contains(Setting.Property.Deprecated)) {
                deprecatedClientSettings.add(affixSetting);
            }
        }
        return List.copyOf(deprecatedClientSettings);
    }
}
