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
 * Persistence envelope for a cloud-managed credential, pairing the public API key {@code id} with
 * the opaque {@link CloudCredential} value. The {@code version} field exists so that a future
 * envelope encoding (e.g. an encrypted payload) can be introduced without breaking documents
 * written today.
 */
public final class PersistedCloudCredential implements Writeable, ToXContentObject, Releasable {

    public static final int CURRENT_VERSION = 1;

    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    private static final ConstructingObjectParser<PersistedCloudCredential, Void> PARSER = new ConstructingObjectParser<>(
        "persisted_cloud_credential",
        true,
        args -> new PersistedCloudCredential((int) args[0], (String) args[1], (CloudCredential) args[2])
    );

    static {
        PARSER.declareInt(constructorArg(), VERSION_FIELD);
        PARSER.declareString(constructorArg(), ID_FIELD);
        PARSER.declareField(
            constructorArg(),
            (p, c) -> new CloudCredential(new SecureString(p.text().toCharArray())),
            VALUE_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    private final int version;
    private final String id;
    private final CloudCredential credential;

    public PersistedCloudCredential(String id, CloudCredential credential) {
        this(CURRENT_VERSION, id, credential);
    }

    private PersistedCloudCredential(int version, String id, CloudCredential credential) {
        if (version <= 0 || version > CURRENT_VERSION) {
            throw new IllegalStateException(
                "unsupported PersistedCloudCredential version [" + version + "]; supported versions are [1.." + CURRENT_VERSION + "]"
            );
        }
        this.version = version;
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.credential = Objects.requireNonNull(credential, "credential must not be null");
    }

    public PersistedCloudCredential(StreamInput in) throws IOException {
        this(in.readVInt(), in.readString(), new CloudCredential(in));
    }

    public int version() {
        return version;
    }

    public String id() {
        return id;
    }

    public CloudCredential credential() {
        return credential;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(version);
        out.writeString(id);
        credential.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VERSION_FIELD.getPreferredName(), version);
        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(VALUE_FIELD.getPreferredName(), credential.value().toString());
        return builder.endObject();
    }

    public static PersistedCloudCredential fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Releases the underlying {@link CloudCredential} and the {@link SecureString} it owns.
     */
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
            return version == other.version && id.equals(other.id) && credential.equals(other.credential);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, id, credential);
    }

    @Override
    public String toString() {
        return "PersistedCloudCredential{version=" + version + ", id=" + id + ", credential=::es_redacted::}";
    }
}
