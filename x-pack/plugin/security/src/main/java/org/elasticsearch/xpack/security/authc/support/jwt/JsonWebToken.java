/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * A class that represents an OpenID Connect ID token according to https://tools.ietf.org/html/rfc7519.
 */
public class JsonWebToken {
    private Map<String, Object> header;
    private Map<String, Object> payload;
    private String signature;

    public JsonWebToken(Map<String, Object> header, Map<String, Object> payload) {
        this.header = header;
        this.payload = payload;
        this.signature = "";
    }

    public Map<String, Object> getHeader() {
        return header;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    /**
     * Encodes the JWT as defined by https://tools.ietf.org/html/rfc7515#section-7
     *
     * @return The serialized JWT
     */
    public String encode() throws IOException {
        // Base64 url encoding is defined in https://tools.ietf.org/html/rfc7515#appendix-C
        String headerString = Base64.getUrlEncoder().withoutPadding().encodeToString(mapToJsonBytes(header));
        String payloadString = Base64.getUrlEncoder().withoutPadding().encodeToString(mapToJsonBytes(payload));
        return headerString + "." + payloadString + "." + signature;
    }

    public void sign(Key key) throws IOException {
        SignatureAlgorithm algorithm = getAlgorithm(header);
        JwtSigner signer = getSigner(algorithm, key);
        if (null == signer) {
            //TODO what kind of Exception?
            throw new IllegalStateException("Wrong algorithm");
        }
        String headerString = Base64.getUrlEncoder().withoutPadding().encodeToString(mapToJsonBytes(header));
        String payloadString = Base64.getUrlEncoder().withoutPadding().encodeToString(mapToJsonBytes(payload));
        final byte[] data = (headerString + "." + payloadString).getBytes(StandardCharsets.UTF_8);
        final byte[] signatureBytes = signer.sign(data);
        signature = Base64.getUrlEncoder().withoutPadding().encodeToString(signatureBytes);
    }

    /**
     * Create a string representation of the JWT. Used for logging and debugging, not for JWT serialization
     *
     * @return a string representation of the JWT
     */
    public String toString() {
        return "{header=" + header + ", payload=" + payload + "}";
    }

    private String mapToJsonString(Map<String, Object> map) throws IOException {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    private byte[] mapToJsonBytes(Map<String, Object> map) throws IOException {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            return BytesReference.toBytes(BytesReference.bytes(builder));
        }
    }

    private JwtSigner getSigner(SignatureAlgorithm algorithm, Key key) {
        if (SignatureAlgorithm.getHmacAlgorithms().contains(algorithm)) {
            return new HmacSigner(algorithm, key);
        } else if (SignatureAlgorithm.getRsaAlgorithms().contains(algorithm)) {
            return new RsaSigner(algorithm, key);
        } else if (SignatureAlgorithm.getEcAlgorithms().contains(algorithm)) {
            return new EcSigner(algorithm, key);
        }
        return null;
    }

    /**
     * Returns the {@link SignatureAlgorithm} that corresponds to the value of the alg claim
     *
     * @param header The {@link Map} containing the parsed header claims
     * @return the SignatureAlgorithm that corresponds to alg
     */
    private SignatureAlgorithm getAlgorithm(Map<String, Object> header) {
        if (header.containsKey("alg")) {
            return SignatureAlgorithm.fromName((String) header.get("alg"));
        } else {
            return null;
        }
    }
}
