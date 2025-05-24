/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.esnative;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.UsernamesField;

import java.util.Set;

public class ClientReservedRealm {

    private static final Set<String> RESERVED_USERNAMES = Set.of(
        UsernamesField.ELASTIC_NAME,
        UsernamesField.DEPRECATED_KIBANA_NAME,
        UsernamesField.KIBANA_NAME,
        UsernamesField.LOGSTASH_NAME,
        UsernamesField.BEATS_NAME,
        UsernamesField.APM_NAME,
        UsernamesField.REMOTE_MONITORING_NAME
    );

    public static boolean isReserved(String username, Settings settings) {
        assert username != null;
        if (isReservedCandidate(username)) {
            return XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings);
        }
        return AnonymousUser.isAnonymousUsername(username, settings);
    }

    public static boolean isReservedCandidate(String username) {
        return RESERVED_USERNAMES.contains(username);
    }
}
