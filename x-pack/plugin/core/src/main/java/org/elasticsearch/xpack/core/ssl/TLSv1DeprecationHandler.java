/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;

import javax.net.ssl.SSLSession;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings.SUPPORTED_PROTOCOLS_TEMPLATE;

/**
 * Handles logging deprecation warnings when a TLSv1.0 SSL connection is used, and that SSL context relies on
 * the default list of supported_protocols (in Elasticsearch 7.0, this list will not include TLS 1.0).
 */
public class TLSv1DeprecationHandler {

    private final String supportedProtocolsSetting;
    private final boolean shouldLogWarnings;
    private final DeprecationLogger deprecationLogger;

    public TLSv1DeprecationHandler(String settingPrefix, Settings settings, Logger baseLogger) {
        if (settingPrefix.length() > 0 && settingPrefix.endsWith("ssl.") == false) {
            throw new IllegalArgumentException("Setting prefix [" + settingPrefix + "] must end in 'ssl.'");
        }
        this.supportedProtocolsSetting = settingPrefix + "supported_protocols";
        this.shouldLogWarnings = SUPPORTED_PROTOCOLS_TEMPLATE.apply(supportedProtocolsSetting).exists(settings) == false;
        if (shouldLogWarnings) {
            deprecationLogger = new DeprecationLogger(baseLogger);
        } else {
            deprecationLogger = null;
        }
    }

    private TLSv1DeprecationHandler(String settingKey, boolean shouldLog, DeprecationLogger logger) {
        this.supportedProtocolsSetting = settingKey;
        this.shouldLogWarnings = shouldLog;
        this.deprecationLogger = logger;
    }

    public static TLSv1DeprecationHandler disabled() {
        return new TLSv1DeprecationHandler(null, false, null);
    }

    public boolean shouldLogWarnings() {
        return shouldLogWarnings;
    }

    public void checkAndLog(SSLSession session, Supplier<String> descriptionSupplier) {
        if (shouldLogWarnings == false) {
            return;
        }
        if ("TLSv1".equals(session.getProtocol())) {
            final String description = descriptionSupplier.get();
            // Use a "LRU" key that is unique per day. That way each description (source address, etc) will be logged once per day.
            final String key = LocalDate.now(ZoneId.of("UTC")) + ":" + description;
            deprecationLogger.deprecatedAndMaybeLog(key,
                "a TLS v1.0 session was used for [{}], " +
                    "this protocol will be disabled by default in a future version. " +
                    "The [{}] setting can be used to control this.",
                description, supportedProtocolsSetting);
        }
    }
}
