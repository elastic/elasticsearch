/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class EnrollmentToken {
    private final String apiKey;
    private final String fingerprint;
    private final String version;
    private final List<String> boundAddress;

    public String getApiKey() {
        return apiKey;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public String getVersion() {
        return version;
    }

    public List<String> getBoundAddress() {
        return boundAddress;
    }

    private static final ParseField API_KEY = new ParseField("key");
    private static final ParseField FINGERPRINT = new ParseField("fgr");
    private static final ParseField VERSION = new ParseField("ver");
    private static final ParseField ADDRESS = new ParseField("adr");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<EnrollmentToken, Void> PARSER = new ConstructingObjectParser<>(
        "enrollment_token",
        false,
        a -> new EnrollmentToken((String) a[0], (String) a[1], (String) a[2], (List<String>) a[3])
    );

    static {
        PARSER.declareString(constructorArg(), API_KEY);
        PARSER.declareString(constructorArg(), FINGERPRINT);
        PARSER.declareString(constructorArg(), VERSION);
        PARSER.declareStringArray(constructorArg(), ADDRESS);
    }

    /**
     * Create an EnrollmentToken
     *
     * @param apiKey         API Key credential in the form apiKeyId:ApiKeySecret to be used for enroll calls
     * @param fingerprint    hex encoded SHA256 fingerprint of the HTTP CA cert
     * @param version        node version number
     * @param boundAddress   IP Addresses and port numbers for the interfaces where the Elasticsearch node is listening on
     */
    public EnrollmentToken(String apiKey, String fingerprint, String version, List<String> boundAddress) {
        this.apiKey = Objects.requireNonNull(apiKey);
        this.fingerprint = Objects.requireNonNull(fingerprint);
        this.version = Objects.requireNonNull(version);
        this.boundAddress = Objects.requireNonNull(boundAddress);
    }

    public String getRaw() throws Exception {
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("ver", version);
        builder.startArray("adr");
        for (String bound_address : boundAddress) {
            builder.value(bound_address);
        }
        builder.endArray();
        builder.field("fgr", fingerprint);
        builder.field("key", apiKey);
        builder.endObject();
        return Strings.toString(builder);
    }

    public String getEncoded() throws Exception {
        final String jsonString = getRaw();
        return Base64.getUrlEncoder().encodeToString(jsonString.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Decodes and parses an enrollment token from its serialized form (created with {@link EnrollmentToken#getEncoded()}
     * @param encoded The Base64 encoded JSON representation of the enrollment token
     * @return the parsed EnrollmentToken
     * @throws IOException when failing to decode the serialized token
     */
    public static EnrollmentToken decodeFromString(String encoded) throws IOException {
        if (Strings.isNullOrEmpty(encoded)) {
            throw new IOException("Cannot decode enrollment token from an empty string");
        }
        final XContentParser jsonParser = JsonXContent.jsonXContent.createParser(
            XContentParserConfiguration.EMPTY,
            Base64.getDecoder().decode(encoded)
        );
        return EnrollmentToken.PARSER.parse(jsonParser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrollmentToken that = (EnrollmentToken) o;
        return apiKey.equals(that.apiKey)
            && fingerprint.equals(that.fingerprint)
            && version.equals(that.version)
            && boundAddress.equals(that.boundAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(apiKey, fingerprint, version, boundAddress);
    }
}
