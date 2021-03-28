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
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.function.Consumer;

public class TlsRuntimeCheck {

    private static final Logger logger = LogManager.getLogger(TlsRuntimeCheck.class);

    private final Settings settings;
    private final boolean httpTlsEnabled;
    private final boolean transportTlsEnabled;

    public TlsRuntimeCheck(Settings settings) {
        this.settings = settings;
        this.httpTlsEnabled = XPackSettings.HTTP_SSL_ENABLED.get(settings);
        this.transportTlsEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
    }

    public void checkTlsThenExecute(Consumer<Exception> exceptionConsumer, String featureName, Runnable andThen) {
        if (false == httpTlsEnabled || false == transportTlsEnabled) {
            final ParameterizedMessage message = new ParameterizedMessage(
                "[{}] requires TLS for both HTTP and Transport, " +
                    "but got HTTP TLS: [{}] and Transport TLS: [{}]", featureName, httpTlsEnabled, transportTlsEnabled);
            logger.debug(message);
            exceptionConsumer.accept(new ElasticsearchException(message.getFormattedMessage()));
        } else {
            andThen.run();
        }
    }
}
