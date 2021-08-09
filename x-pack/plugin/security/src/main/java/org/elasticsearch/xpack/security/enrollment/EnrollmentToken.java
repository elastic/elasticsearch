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

public class EnrollmentToken {
    private final String apiKey;
    private final String fingerprint;
    private final String version;
    private final List<String > bound_address;

    public String getApiKey() { return apiKey; }
    public String getFingerprint() { return fingerprint; }
    public String getVersion() { return version; }
    public List<String> getBound_address() { return bound_address; }

    public EnrollmentToken(String apiKey, String fingerprint, String version, List<String> bound_address) {
        this.apiKey = apiKey;
        this.fingerprint = fingerprint;
        this.version = version;
        this.bound_address = bound_address;
    }

    public String encode() throws Exception{
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("ver", version);
        builder.startArray("adr");
        for (String bound_address : bound_address) {
            builder.value(bound_address);
        }
        builder.endArray();
        builder.field("fgr", fingerprint);
        builder.field("key", apiKey);
        builder.endObject();
        final String jsonString = Strings.toString(builder);
        return Base64.getUrlEncoder().encodeToString(jsonString.getBytes(StandardCharsets.UTF_8));
    }
}
