/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.gce;

import com.google.api.client.http.HttpTransport;

import org.elasticsearch.cloud.gce.GceInstancesServiceImpl;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.security.GeneralSecurityException;

public class GceInstancesServiceMock extends GceInstancesServiceImpl {

    protected HttpTransport mockHttpTransport;

    public GceInstancesServiceMock(Settings settings) {
        super(settings);
    }

    @Override
    protected HttpTransport getGceHttpTransport() throws GeneralSecurityException, IOException {
        if (this.mockHttpTransport == null) {
            this.mockHttpTransport = GceMockUtils.configureMock();
        }
        return this.mockHttpTransport;
    }
}
