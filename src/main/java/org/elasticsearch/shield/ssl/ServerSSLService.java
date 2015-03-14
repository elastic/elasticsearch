/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.ShieldSettingsFilter;

public class ServerSSLService extends AbstractSSLService {

    @Inject
    public ServerSSLService(Settings settings, ShieldSettingsFilter settingsFilter) {
        super(settings);

        // we need to filter out all this sensitive information from all rest
        // responses
        settingsFilter.filterOut("shield.ssl.*");
    }

    @Override
    protected SSLSettings sslSettings(Settings customSettings) {
        SSLSettings sslSettings = new SSLSettings(customSettings, componentSettings);

        if (sslSettings.keyStorePath == null) {
            throw new ShieldSettingsException("no keystore configured");
        }
        if (sslSettings.keyStorePassword == null) {
            throw new ShieldSettingsException("no keystore password configured");
        }

        assert sslSettings.trustStorePath != null;
        if (sslSettings.trustStorePassword == null) {
            throw new ShieldSettingsException("no truststore password configured");
        }
        return sslSettings;
    }
}
