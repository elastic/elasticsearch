/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import java.util.Locale;

/**
 * Represents the verification mode to be used for SSL connections.
 */
public enum VerificationMode {
    NONE {
        @Override
        public boolean isHostnameVerificationEnabled() {
            return false;
        }

        @Override
        public boolean isCertificateVerificationEnabled() {
            return false;
        }
    },
    CERTIFICATE {
        @Override
        public boolean isHostnameVerificationEnabled() {
            return false;
        }

        @Override
        public boolean isCertificateVerificationEnabled() {
            return true;
        }
    },
    FULL {
        @Override
        public boolean isHostnameVerificationEnabled() {
            return true;
        }

        @Override
        public boolean isCertificateVerificationEnabled() {
            return true;
        }
    };

    /**
     * @return true if hostname verification is enabled
     */
    public abstract boolean isHostnameVerificationEnabled();

    /**
     * @return true if certificate verification is enabled
     */
    public abstract boolean isCertificateVerificationEnabled();

    public static VerificationMode parse(String value) {
        assert value != null;
        switch (value.toLowerCase(Locale.ROOT)) {
            case "none":
                return NONE;
            case "certificate":
                return CERTIFICATE;
            case "full":
                return FULL;
            default:
                throw new IllegalArgumentException("could not resolve verification mode. unknown value [" + value + "]");
        }
    }
}