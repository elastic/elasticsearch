/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.gce;

import com.google.api.client.http.HttpTransport;

import org.elasticsearch.cloud.gce.GceMetadataService;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Mock for GCE Metadata Service
 */
public class GceMetadataServiceMock extends GceMetadataService {

    protected HttpTransport mockHttpTransport;

    public GceMetadataServiceMock(Settings settings) {
        super(settings);
        this.mockHttpTransport = GceMockUtils.configureMock();
    }

    @Override
    protected HttpTransport getGceHttpTransport() throws GeneralSecurityException, IOException {
        return this.mockHttpTransport;
    }
}
