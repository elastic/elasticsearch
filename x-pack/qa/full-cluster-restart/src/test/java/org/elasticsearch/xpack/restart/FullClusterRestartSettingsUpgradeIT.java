/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.restart;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class FullClusterRestartSettingsUpgradeIT extends org.elasticsearch.upgrades.FullClusterRestartSettingsUpgradeIT {

    @Override
    protected Settings restClientSettings() {
        final String token =
                "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

}
