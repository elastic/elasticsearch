/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.xpack.core.ssl.extension;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.core.ssl.extension.SslProfileExtension;

import java.util.Set;

public class TestSslProfile implements SslProfileExtension {

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public Set<String> getSettingPrefixes() {
        return Set.of("test.ssl");
    }

    @Override
    public void applyProfile(String prefix, SslProfile profile) {
        profile.addReloadListener(p -> logger.info("TEST SSL PROFILE RELOADED [{}] [{}]", prefix, p));
    }
}
