/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Persistence envelope for a cloud-managed (UIAM) credential, pairing the public key {@code id}
 * with the opaque {@link CloudCredential}.
 */
public final class PersistedCloudCredential implements Writeable, ToXContentObject, Releasable {

    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    private static final ConstructingObjectParser<PersistedCloudCredential, Void> PARSER = new ConstructingObjectParser<>(
        "persisted_cloud_credential",
        true,
        args -> new PersistedCloudCredential((String) args[0], (CloudCredential) args[1])
    );

    static {
        PARSER.declareString(constructorArg(), ID_FIELD);
        PARSER.declareField(
            constructorArg(),
            (p, c) -> new CloudCredential(new SecureString(p.text().toCharArray())),
            VALUE_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    private final String id;
    private final CloudCredential credential;

    public PersistedCloudCredential(String id, CloudCredential credential) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.credential = Objects.requireNonNull(credential, "credential must not be null");
    }

    public PersistedCloudCredential(StreamInput in) throws IOException {
        this(in.readString(), new CloudCredential(in));
    }

    public String id() {
        return id;
    }

    public CloudCredential credential() {
        return credential;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        credential.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(VALUE_FIELD.getPreferredName(), credential.value().toString());
        return builder.endObject();
    }

    public static PersistedCloudCredential fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void close() {
        credential.close();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof PersistedCloudCredential other) {
            return id.equals(other.id) && credential.equals(other.credential);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, credential);
    }

    @Override
    public String toString() {
        return "PersistedCloudCredential{id=" + id + ", credential=::es_redacted::}";
    }
}
