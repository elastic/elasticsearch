/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.core.Nullable;

public enum ApiKeyType {

    DEFAULT() {
        @Override
        public String getDocType() {
            return "api_key";
        }

        @Override
        public String getCredentialsPrefix() {
            return "";
        }
    },
    CCS() {
        @Override
        public String getDocType() {
            return "api_key_ccs";
        }

        @Override
        public String getCredentialsPrefix() {
            return "ccs_";
        }
    };

    public abstract String getDocType();

    public abstract String getCredentialsPrefix();

    public static ApiKeyType fromDocType(@Nullable String docType) {
        if (docType == null) {
            return DEFAULT;
        }
        return switch (docType) {
            case "api_key" -> DEFAULT;
            case "api_key_ccs" -> CCS;
            default -> throw new IllegalArgumentException("unknown doc type [" + docType + "]");
        };
    }

    public static ApiKeyType fromCredentialsPrefix(String prefix) {
        return switch (prefix) {
            case "ccs_" -> CCS;
            default -> throw new IllegalArgumentException("unknown credentials prefix [" + prefix + "]");
        };
    }
}
