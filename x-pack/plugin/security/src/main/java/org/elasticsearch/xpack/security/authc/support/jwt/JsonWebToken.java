/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * A class that represents a JWT according to https://tools.ietf.org/html/rfc7519
 */
public class JsonWebToken {
    private Map<String, Object> header;
    private Map<String, Object> payload;
    private String signature;

    public JsonWebToken(Map<String, Object> header, Map<String, Object> payload) {
        this.header = header;
        this.payload = payload;
    }

    /**
     * Encodes the JWT as defined by https://tools.ietf.org/html/rfc7515#section-7
     *
     * @return
     */
    public String encode() {
        try {
            String headerString = Base64.getEncoder().encodeToString(mapToJsonBytes(header));
            String payloadString = Base64.getEncoder().encodeToString(mapToJsonBytes(payload));
            String signatureString = Strings.hasText(signature) ?
                Base64.getEncoder().encodeToString(signature.getBytes(StandardCharsets.UTF_8.name())) :
                "";
            return headerString + "." + payloadString + "." + signatureString;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Create a string representation of the JWT. Used for logging and debugging, not for JWT serialization
     *
     * @return a string representation of the JWT
     */
    public String toString() {
        return "{header=" + header + ", payload=" + payload + ", signature=" + signature + "}";
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
}
