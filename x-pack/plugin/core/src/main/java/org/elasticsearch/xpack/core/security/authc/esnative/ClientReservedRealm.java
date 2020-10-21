/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.esnative;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.UsernamesField;

public class ClientReservedRealm {

    public static boolean isReserved(String username, Settings settings) {
        assert username != null;
        switch (username) {
            case UsernamesField.ELASTIC_NAME:
            case UsernamesField.DEPRECATED_KIBANA_NAME:
            case UsernamesField.KIBANA_NAME:
            case UsernamesField.LOGSTASH_NAME:
            case UsernamesField.BEATS_NAME:
            case UsernamesField.APM_NAME:
            case UsernamesField.REMOTE_MONITORING_NAME:
                return XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings);
            default:
                return AnonymousUser.isAnonymousUsername(username, settings);
        }
    }
}
