/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

public class EnrollmentToken {
    private final String apiKey;
    private final String fingerprint;
    private final String version;
    private final List<String > boundAddress;

    public String getApiKey() { return apiKey; }
    public String getFingerprint() { return fingerprint; }
    public String getVersion() { return version; }
    public List<String> getBoundAddress() { return boundAddress; }

    /**
     * Create an EnrollmentToken
     *
     * @param apiKey         API Key credential in the form apiKeyId:ApiKeySecret to be used for enroll calls
     * @param fingerprint    hex encoded SHA256 fingerprint of the HTTP CA cert
     * @param version        node version number
     * @param boundAddress   IP Addresses and port numbers for the interface where the Elasticsearch node is listening on
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

    public String encode() throws Exception {
        final String jsonString = getRaw();
        return Base64.getUrlEncoder().encodeToString(jsonString.getBytes(StandardCharsets.UTF_8));
    }
}
