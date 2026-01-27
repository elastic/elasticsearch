/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authc.Realm;

import java.util.Map;

public class CloudIamSecurityExtension implements SecurityExtension {
    @Override
    public Map<String, Realm.Factory> getRealms(SecurityComponents components) {
        return Map.of(
            CloudIamRealmSettings.TYPE,
            config -> {
                String mode = config.getSetting(CloudIamRealmSettings.AUTH_MODE, () -> "aliyun");
                IamClient iamClient = "mock".equalsIgnoreCase(mode) ? new MockIamClient(config) : new AliyunStsClient(config);
                return new CloudIamRealm(config, components.threadPool(), components.roleMapper(), iamClient);
            }
        );
    }
}
