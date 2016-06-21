/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportService;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

// this class sits in org.elasticsearch.transport so that TransportService.requestHandlers is visible
public class SecurityServerTransportServiceTests extends SecurityIntegTestCase {
    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .put(Security.enabledSetting(), true)
                .build();
    }

    public void testSecurityServerTransportServiceWrapsAllHandlers() {
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            assertThat(transportService, instanceOf(SecurityServerTransportService.class));
            for (Map.Entry<String, RequestHandlerRegistry> entry : transportService.requestHandlers.entrySet()) {
                assertThat(
                        "handler not wrapped by " + SecurityServerTransportService.ProfileSecuredRequestHandler.class +
                                "; do all the handler registration methods have overrides?",
                        entry.getValue().toString(),
                        startsWith(SecurityServerTransportService.ProfileSecuredRequestHandler.class.getName() + "@")
                );
            }
        }
    }
}
