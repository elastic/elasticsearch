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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Arrays;
import java.util.function.Consumer;

public class HttpTlsRuntimeCheck {

    private static final Logger logger = LogManager.getLogger(HttpTlsRuntimeCheck.class);

    private final Settings settings;
    private final Boolean httpTlsEnabled;
    private final boolean enforce;

    public HttpTlsRuntimeCheck(Settings settings) {
        this(settings, null);
    }

    public HttpTlsRuntimeCheck(Settings settings, TransportService transportService) {
        this.settings = settings;
        this.httpTlsEnabled = XPackSettings.HTTP_SSL_ENABLED.get(settings);
        if (transportService != null) {
            final boolean boundToLocal = Arrays.stream(transportService.boundAddress().boundAddresses())
                .allMatch(b -> b.address().getAddress().isLoopbackAddress())
                && transportService.boundAddress().publishAddress().address().getAddress().isLoopbackAddress();
            this.enforce = false == boundToLocal && false == "single-node".equals(DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings));
        } else {
            enforce = true;
        }

    }

    public void checkTlsThenExecute(Consumer<Exception> exceptionConsumer, String featureName, Runnable andThen) {
        if (enforce && false == httpTlsEnabled) {
            final ParameterizedMessage message = new ParameterizedMessage(
                "[{}] requires TLS for the HTTP interface", featureName);
            logger.debug(message);
            exceptionConsumer.accept(new ElasticsearchException(message.getFormattedMessage()));
        } else {
            andThen.run();
        }
    }
}
