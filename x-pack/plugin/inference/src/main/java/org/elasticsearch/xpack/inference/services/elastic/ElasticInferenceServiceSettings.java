/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Class encapsulating any global setting for the EIS integration.
 */
public class ElasticInferenceServiceSettings {

    public static final Setting<String> EIS_GATEWAY_URL = Setting.simpleString(
        "xpack.inference.eis.gateway.url",
        new EisGatewayURLValidator(),
        Setting.Property.NodeScope
    );

    private static final Logger log = LogManager.getLogger(ElasticInferenceServiceSettings.class);

    /**
     * Class to validate the EIS Gateway url set via `xpack.inference.eis.gateway.url`.
     */
    public static class EisGatewayURLValidator implements Setting.Validator<String> {

        private static final Set<String> VALID_EIS_GATEWAY_SCHEMES = Set.of("http", "https");

        @Override
        public void validate(String value) {
            if (Objects.isNull(value) || value.isEmpty()) {
                // No validation needed, if eis-gateway URL is not set
                log.debug("eis-gateway url not set. Skipping validation");
                return;
            }

            try {
                var uri = new URI(value);
                var scheme = uri.getScheme();

                if (scheme == null || VALID_EIS_GATEWAY_SCHEMES.contains(scheme) == false) {
                    throw new IllegalArgumentException(
                        "["
                            + scheme
                            + "] is not a valid URI scheme for the setting ["
                            + ElasticInferenceServiceSettings.EIS_GATEWAY_URL.getKey()
                            + "]. Use one of ["
                            + String.join(",", VALID_EIS_GATEWAY_SCHEMES)
                            + "]"
                    );
                }
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("[" + e.getInput() + "] is not a valid URI", e);
            }
        }
    }

    // Adjust this variable to be volatile, if the setting can be updated at some point in time
    private final String eisGatewayUrl;

    public ElasticInferenceServiceSettings(Settings settings) {
        eisGatewayUrl = EIS_GATEWAY_URL.get(settings);
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(EIS_GATEWAY_URL);
    }

    public String getEisGatewayUrl() {
        return eisGatewayUrl;
    }
}
