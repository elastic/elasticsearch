/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.namedcredentials;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

/**
 * A named credential as presented over the API. The {@code auth} map is {@code null} everywhere
 * except on the {@code _decrypt} path; a null auth renders as the {@link #REDACTED} sentinel so
 * that no read path can leak secrets by accident.
 */
public record NamedCredential(
    String name,
    CredentialAuthType authType,
    @Nullable String url,
    Map<String, String> config,
    @Nullable Map<String, String> auth,
    long createdAtMillis,
    long updatedAtMillis
) implements ToXContentObject {

    public static final String REDACTED = "::es_redacted::";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("auth_type", authType.typeName());
        if (url != null) {
            builder.field("url", url);
        }
        builder.field("config", config);
        if (auth != null) {
            builder.field("auth", auth);
        } else {
            builder.field("auth", REDACTED);
        }
        builder.field("created_at", Instant.ofEpochMilli(createdAtMillis).toString());
        builder.field("updated_at", Instant.ofEpochMilli(updatedAtMillis).toString());
        builder.endObject();
        return builder;
    }
}
