/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.AbstractBytesReference;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class RemoteAccessAuthentication {
    public static final String REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY = "_remote_access_authentication";
    private final Authentication authentication;
    private final List<RoleDescriptorsBytes> roleDescriptorsBytes;

    public RemoteAccessAuthentication(Authentication authentication, RoleDescriptorsIntersection roleDescriptorsIntersection)
        throws IOException {
        this(authentication, roleDescriptorsToBytes(roleDescriptorsIntersection));
    }

    private RemoteAccessAuthentication(Authentication authentication, List<RoleDescriptorsBytes> roleDescriptorsBytes) {
        this.authentication = authentication;
        this.roleDescriptorsBytes = roleDescriptorsBytes;
    }

    public void writeToContext(final ThreadContext ctx) throws IOException {
        ctx.putHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY, encode());
    }

    public static RemoteAccessAuthentication readFromContext(final ThreadContext ctx) throws IOException {
        return decode(ctx.getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY));
    }

    private String encode() throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(authentication.getEffectiveSubject().getVersion());
        Version.writeVersion(authentication.getEffectiveSubject().getVersion(), out);
        authentication.writeTo(out);
        out.writeCollection(roleDescriptorsBytes, (o, rd) -> rd.writeTo(o));
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
    }

    private static List<RoleDescriptorsBytes> roleDescriptorsToBytes(RoleDescriptorsIntersection rdsIntersection) throws IOException {
        final List<RoleDescriptorsBytes> bytes = new ArrayList<>();
        for (Set<RoleDescriptor> roleDescriptors : rdsIntersection.roleDescriptorsList()) {
            bytes.add(RoleDescriptorsBytes.fromRoleDescriptors(roleDescriptors));
        }
        return bytes;
    }

    private static RemoteAccessAuthentication decode(final String header) throws IOException {
        Objects.requireNonNull(header);
        final byte[] bytes = Base64.getDecoder().decode(header);
        final StreamInput in = StreamInput.wrap(bytes);
        final Version version = Version.readVersion(in);
        in.setVersion(version);
        final Authentication authentication = new Authentication(in);
        final List<RoleDescriptorsBytes> roleDescriptorsBytesIntersection = in.readImmutableList(RoleDescriptorsBytes::new);
        return new RemoteAccessAuthentication(authentication, roleDescriptorsBytesIntersection);
    }

    public Authentication authentication() {
        return authentication;
    }

    public List<RoleDescriptorsBytes> roleDescriptorsBytesIntersection() {
        return roleDescriptorsBytes;
    }

    public static final class RoleDescriptorsBytes extends AbstractBytesReference implements Writeable {
        private final BytesReference rawBytes;

        public RoleDescriptorsBytes(BytesReference rawBytes) {
            this.rawBytes = rawBytes;
        }

        public RoleDescriptorsBytes(StreamInput streamInput) throws IOException {
            this(streamInput.readBytesReference());
        }

        public Set<RoleDescriptor> parse() {
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, rawBytes, XContentType.JSON)) {
                final List<RoleDescriptor> roleDescriptors = new ArrayList<>();
                parser.nextToken();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    parser.nextToken();
                    final String roleName = parser.currentName();
                    roleDescriptors.add(RoleDescriptor.parse(roleName, parser, false));
                }
                return Set.copyOf(roleDescriptors);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        static RoleDescriptorsBytes fromRoleDescriptors(final Set<RoleDescriptor> roleDescriptors) throws IOException {
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            for (RoleDescriptor roleDescriptor : roleDescriptors) {
                builder.field(roleDescriptor.getName(), roleDescriptor);
            }
            builder.endObject();
            return new RoleDescriptorsBytes(BytesReference.bytes(builder));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(rawBytes);
        }

        public BytesReference rawBytes() {
            return rawBytes;
        }

        @Override
        public byte get(int index) {
            return rawBytes.get(0);
        }

        @Override
        public int length() {
            return rawBytes.length();
        }

        @Override
        public BytesReference slice(int from, int length) {
            return rawBytes.slice(from, length);
        }

        @Override
        public long ramBytesUsed() {
            return rawBytes.ramBytesUsed();
        }

        @Override
        public BytesRef toBytesRef() {
            return rawBytes.toBytesRef();
        }
    }
}
