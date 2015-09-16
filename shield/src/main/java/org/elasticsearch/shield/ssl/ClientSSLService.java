/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.nio.file.Path;

@SuppressForbidden(reason = "we don't have the environment to resolve files from when running in a transport client")
public class ClientSSLService extends AbstractSSLService {

    @Inject
    public ClientSSLService(Settings settings) {
        super(settings, null);
    }

    @Inject(optional = true)
    public void setEnvironment(Environment environment) {
        this.env = environment;
    }

    @Override
    protected SSLSettings sslSettings(Settings customSettings) {
        SSLSettings sslSettings = new SSLSettings(customSettings, settings);

        if (sslSettings.keyStorePath != null) {
            if (sslSettings.keyStorePassword == null) {
                throw new IllegalArgumentException("no keystore password configured");
            }
        }

        if (sslSettings.trustStorePath != null) {
            if (sslSettings.trustStorePassword == null) {
                throw new IllegalArgumentException("no truststore password configured");
            }
        }

        return sslSettings;
    }

    @Override
    protected Path resolvePath(String location) {
        if (env == null) {
            return PathUtils.get(Strings.cleanPath(location));
        }
        return super.resolvePath(location);
    }
}
