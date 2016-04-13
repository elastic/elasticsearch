/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

public class ServerSSLService extends AbstractSSLService {

    @Inject
    public ServerSSLService(Settings settings, Environment environment) {
        super(settings, environment);
    }

    @Override
    protected SSLSettings sslSettings(Settings customSettings) {
        SSLSettings sslSettings = new SSLSettings(customSettings, settings);

        if (sslSettings.keyStorePath == null) {
            throw new IllegalArgumentException("no keystore configured");
        }
        if (sslSettings.keyStorePassword == null) {
            throw new IllegalArgumentException("no keystore password configured");
        }
        assert sslSettings.keyPassword != null;

        assert sslSettings.trustStorePath != null;
        if (sslSettings.trustStorePassword == null) {
            throw new IllegalArgumentException("no truststore password configured");
        }
        return sslSettings;
    }
}
