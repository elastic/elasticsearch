/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Base64;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public record ParentIndexActionAuthorization(Version version, String action, boolean granted) implements Writeable, ToXContent {

    public static final String THREAD_CONTEXT_KEY = "_xpack_security_authorization";

    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField ACTION = new ParseField("action");
    private static final ParseField GRANTED = new ParseField("granted");

    private static final ConstructingObjectParser<ParentIndexActionAuthorization, String> PARSER = new ConstructingObjectParser<>(
        "authorization",
        true,
        (a) -> new ParentIndexActionAuthorization((Version) a[0], (String) a[1], (boolean) a[2])
    );
    static {
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> parseVersion(p.text()),
            VERSION,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareString(constructorArg(), ACTION);
        PARSER.declareBoolean(constructorArg(), GRANTED);
    }
    private static Version parseVersion(String version) {
        if (version == null || version.isBlank()) {
            throw new IllegalArgumentException(VERSION.getPreferredName() + " must not be empty");
        }
        return Version.fromString(version);
    }

    /**
     * Reads an {@link ParentIndexActionAuthorization} from a {@link StreamInput}
     *
     * @param in the {@link StreamInput} to read from
     * @return {@link ParentIndexActionAuthorization}
     * @throws IOException if I/O operation fails
     */
    public static ParentIndexActionAuthorization readFrom(StreamInput in) throws IOException {
        Version version = Version.readVersion(in);
        String action = in.readString();
        boolean granted = in.readBoolean();
        return new ParentIndexActionAuthorization(version, action, granted);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Version.writeVersion(version, out);
        out.writeString(action);
        out.writeBoolean(granted);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(VERSION.getPreferredName(), version);
        builder.field(ACTION.getPreferredName(), action);
        builder.field(GRANTED.getPreferredName(), granted);
        return builder;
    }

    /**
     * Reads {@link ParentIndexActionAuthorization} from {@link XContentParser}
     *
     * @param parser {@link XContentParser}
     * @return {@link ParentIndexActionAuthorization}
     * @throws IOException if I/O operation fails
     */
    public static ParentIndexActionAuthorization fromXContent(final XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Nullable
    public static ParentIndexActionAuthorization readFromThreadContext(ThreadContext context) throws IOException {
        ParentIndexActionAuthorization authorization = context.getTransient(THREAD_CONTEXT_KEY);
        if (authorization != null) {
            assert context.getHeader(THREAD_CONTEXT_KEY) != null;
            return authorization;
        }

        final String header = context.getHeader(THREAD_CONTEXT_KEY);
        if (header == null) {
            return null;
        }

        byte[] bytes = Base64.getDecoder().decode(header);
        StreamInput input = StreamInput.wrap(bytes);
        Version version = Version.readVersion(input);
        input.setVersion(version);
        authorization = readFrom(input);
        return authorization;
    }

    /**
     * Writes the authorization to the context. There must not be an existing authorization in the context and if there is an
     * {@link IllegalStateException} will be thrown.
     */
    public void writeToThreadContext(ThreadContext context) throws IOException {
        ensureContextDoesNotContainAuthorization(context);
        String header = this.encode();
        assert header != null : "Authorization object encoded to null";
        context.putTransient(THREAD_CONTEXT_KEY, this);
        context.putHeader(THREAD_CONTEXT_KEY, header);
    }

    private void ensureContextDoesNotContainAuthorization(ThreadContext context) {
        if (context.getTransient(THREAD_CONTEXT_KEY) != null) {
            if (context.getHeader(THREAD_CONTEXT_KEY) == null) {
                throw new IllegalStateException("authorization present as a transient ([" + THREAD_CONTEXT_KEY + "]) but not a header");
            }
            throw new IllegalStateException("authorization ([" + THREAD_CONTEXT_KEY + "]) is already present in the context");
        }
    }

    public String encode() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(version);
        Version.writeVersion(version, output);
        writeTo(output);
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(output.bytes()));
    }
}
