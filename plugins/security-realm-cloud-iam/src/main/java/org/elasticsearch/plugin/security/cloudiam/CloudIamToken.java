/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class CloudIamToken implements AuthenticationToken {
    private static final String PARAM_ACTION = "Action";
    private static final String PARAM_VERSION = "Version";
    private static final String PARAM_ACCESS_KEY_ID = "AccessKeyId";
    private static final String PARAM_SIGNATURE = "Signature";
    private static final String PARAM_SIGNATURE_METHOD = "SignatureMethod";
    private static final String PARAM_SIGNATURE_VERSION = "SignatureVersion";
    private static final String PARAM_SIGNATURE_NONCE = "SignatureNonce";
    private static final String PARAM_TIMESTAMP = "Timestamp";
    private static final String PARAM_SECURITY_TOKEN = "SecurityToken";

    private final String accessKeyId;
    private final Instant timestamp;
    private final String nonce;
    private String signature;
    private final String sessionToken;
    private final Map<String, String> signedParams;
    private final boolean valid;
    private final String validationError;

    private CloudIamToken(
        String accessKeyId,
        Instant timestamp,
        String nonce,
        String signature,
        String sessionToken,
        Map<String, String> signedParams,
        boolean valid,
        String validationError
    ) {
        this.accessKeyId = accessKeyId;
        this.timestamp = timestamp;
        this.nonce = nonce;
        this.signature = signature;
        this.sessionToken = sessionToken;
        this.signedParams = signedParams;
        this.valid = valid;
        this.validationError = validationError;
    }

    public static CloudIamToken fromHeaders(String signedHeader, int signedHeaderMaxBytes) {
        if (signedHeader == null || signedHeader.isBlank()) {
            return invalid("missing signed header");
        }
        if (signedHeader.length() > signedHeaderMaxBytes * 2L) {
            return invalid("signed header too large");
        }
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(signedHeader);
        } catch (IllegalArgumentException e) {
            return invalid("invalid signed header");
        }
        if (decoded.length > signedHeaderMaxBytes) {
            return invalid("signed header too large");
        }
        Map<String, String> params;
        try {
            params = parseSignedParams(decoded);
        } catch (Exception e) {
            return invalid("invalid signed json");
        }
        String accessKeyId = params.get(PARAM_ACCESS_KEY_ID);
        String timestampRaw = params.get(PARAM_TIMESTAMP);
        String nonce = params.get(PARAM_SIGNATURE_NONCE);
        String signature = params.get(PARAM_SIGNATURE);
        String sessionToken = params.get(PARAM_SECURITY_TOKEN);
        if (accessKeyId == null || timestampRaw == null || nonce == null || signature == null) {
            return invalid("missing required signed fields");
        }
        Instant timestamp;
        try {
            timestamp = Instant.parse(timestampRaw);
        } catch (DateTimeParseException e) {
            return invalid("invalid timestamp");
        }
        return new CloudIamToken(accessKeyId, timestamp, nonce, signature, sessionToken, params, true, null);
    }

    private static CloudIamToken invalid(String reason) {
        return new CloudIamToken(null, null, null, null, null, null, false, reason);
    }

    private static Map<String, String> parseSignedParams(byte[] decodedJson) throws Exception {
        Tuple<XContentType, Map<String, Object>> parsed = XContentHelper.convertToMap(
            new BytesArray(decodedJson),
            false,
            XContentType.JSON
        );
        Map<String, Object> raw = parsed.v2();
        Map<String, String> params = new HashMap<>();
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            if (entry.getValue() instanceof String value) {
                params.put(entry.getKey(), value);
            } else {
                params.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return params;
    }

    public boolean isValid() {
        return valid;
    }

    public String validationError() {
        return validationError;
    }

    public String accessKeyId() {
        return accessKeyId;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public String nonce() {
        return nonce;
    }

    public String signature() {
        return signature;
    }

    public String sessionToken() {
        return sessionToken;
    }

    public Map<String, String> signedParams() {
        return signedParams;
    }

    @Override
    public String principal() {
        return accessKeyId == null ? "" : accessKeyId;
    }

    @Override
    public Object credentials() {
        return signature;
    }

    @Override
    public void clearCredentials() {
        signature = null;
    }
}
