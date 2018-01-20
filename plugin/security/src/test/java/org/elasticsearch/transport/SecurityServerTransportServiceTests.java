/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.transport;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

// this class sits in org.elasticsearch.transport so that TransportService.requestHandlers is visible
public class SecurityServerTransportServiceTests extends SecurityIntegTestCase {
    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .put(XPackSettings.SECURITY_ENABLED.getKey(), true)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Collection<Class<? extends Plugin>> mockPlugins = super.getMockPlugins();
        // no handler wrapping here we check the requestHandlers below and this plugin wraps it
        return mockPlugins.stream().filter(p -> p != AssertingTransportInterceptor.TestPlugin.class).collect(Collectors.toList());
    }

    public void testSecurityServerTransportServiceWrapsAllHandlers() {
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            for (Map.Entry<String, RequestHandlerRegistry> entry : transportService.requestHandlers.entrySet()) {
                RequestHandlerRegistry handler = entry.getValue();
                assertEquals(
                        "handler not wrapped by " + SecurityServerTransportInterceptor.ProfileSecuredRequestHandler.class +
                                "; do all the handler registration methods have overrides?",
                        handler.toString(),
                        "ProfileSecuredRequestHandler{action='" + handler.getAction() + "', executorName='" + handler.getExecutor()
                                + "', forceExecution=" + handler.isForceExecution() + "}");
            }
        }
    }
}
