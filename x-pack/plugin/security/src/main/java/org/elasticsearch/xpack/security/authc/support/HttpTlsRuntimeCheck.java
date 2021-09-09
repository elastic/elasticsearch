/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class HttpTlsRuntimeCheck {

    private static final Logger logger = LogManager.getLogger(HttpTlsRuntimeCheck.class);

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Boolean httpTlsEnabled;
    private final SetOnce<Transport> transportReference;
    private final Boolean securityEnabled;
    private final boolean singleNodeDiscovery;
    private boolean enforce;

    public HttpTlsRuntimeCheck(Settings settings, SetOnce<Transport> transportReference) {
        this.transportReference = transportReference;
        this.securityEnabled = XPackSettings.SECURITY_ENABLED.get(settings);
        this.httpTlsEnabled = XPackSettings.HTTP_SSL_ENABLED.get(settings);
        this.singleNodeDiscovery = "single-node".equals(DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings));
    }

    public void checkTlsThenExecute(Consumer<Exception> exceptionConsumer, String featureName, Runnable andThen) {
        // If security is enabled, but TLS is not enabled for the HTTP interface
        if (securityEnabled && false == httpTlsEnabled) {
            if (false == initialized.get()) {
                final Transport transport = transportReference.get();
                if (transport == null) {
                    exceptionConsumer.accept(new ElasticsearchException("transport cannot be null"));
                    return;
                }
                final boolean boundToLocal = Arrays.stream(transport.boundAddress().boundAddresses())
                    .allMatch(b -> b.address().getAddress().isLoopbackAddress())
                    && transport.boundAddress().publishAddress().address().getAddress().isLoopbackAddress();
                this.enforce = false == boundToLocal && false == singleNodeDiscovery;
                initialized.set(true);
            }
            if (enforce) {
                final ParameterizedMessage message = new ParameterizedMessage(
                    "[{}] requires TLS for the HTTP interface", featureName);
                logger.debug(message);
                exceptionConsumer.accept(new ElasticsearchException(message.getFormattedMessage()));
                return;
            }
        }
        andThen.run();
    }
}
