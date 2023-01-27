/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.AbstractBytesReference;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
    private final List<RoleDescriptorsBytes> roleDescriptorsBytesList;

    public RemoteAccessAuthentication(Authentication authentication, RoleDescriptorsIntersection roleDescriptorsIntersection)
        throws IOException {
        this(authentication, toRoleDescriptorsBytesList(roleDescriptorsIntersection));
    }

    private RemoteAccessAuthentication(Authentication authentication, List<RoleDescriptorsBytes> roleDescriptorsBytesList) {
        this.authentication = authentication;
        this.roleDescriptorsBytesList = roleDescriptorsBytesList;
    }

    public void writeToContext(final ThreadContext ctx) throws IOException {
        ctx.putHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY, encode());
    }

    public static RemoteAccessAuthentication readFromContext(final ThreadContext ctx) throws IOException {
        return decode(ctx.getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY));
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    public List<RoleDescriptorsBytes> getRoleDescriptorsBytesList() {
        return roleDescriptorsBytesList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteAccessAuthentication that = (RemoteAccessAuthentication) o;

        if (false == authentication.equals(that.authentication)) return false;
        return roleDescriptorsBytesList.equals(that.roleDescriptorsBytesList);
    }

    @Override
    public int hashCode() {
        int result = authentication.hashCode();
        result = 31 * result + roleDescriptorsBytesList.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RemoteAccessAuthentication{"
            + "authentication="
            + authentication
            + ", roleDescriptorsBytesList="
            + roleDescriptorsBytesList
            + '}';
    }

    private static List<RoleDescriptorsBytes> toRoleDescriptorsBytesList(final RoleDescriptorsIntersection roleDescriptorsIntersection)
        throws IOException {
        // If we ever lift this restriction, we need to ensure that the serialization of each set of role descriptors to raw bytes is
        // deterministic. We can do so by sorting the role descriptors before serializing.
        assert roleDescriptorsIntersection.roleDescriptorsList().stream().noneMatch(rds -> rds.size() > 1)
            : "sets with more than one role descriptor are not supported for remote access authentication";
        final List<RoleDescriptorsBytes> roleDescriptorsBytesList = new ArrayList<>();
        for (Set<RoleDescriptor> roleDescriptors : roleDescriptorsIntersection.roleDescriptorsList()) {
            roleDescriptorsBytesList.add(RoleDescriptorsBytes.fromRoleDescriptors(roleDescriptors));
        }
        return roleDescriptorsBytesList;
    }

    public String encode() throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(authentication.getEffectiveSubject().getTransportVersion());
        TransportVersion.writeVersion(authentication.getEffectiveSubject().getTransportVersion(), out);
        authentication.writeTo(out);
        out.writeCollection(roleDescriptorsBytesList, StreamOutput::writeBytesReference);
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
    }

    public static RemoteAccessAuthentication decode(final String header) throws IOException {
        Objects.requireNonNull(header);
        final byte[] bytes = Base64.getDecoder().decode(header);
        final StreamInput in = StreamInput.wrap(bytes);
        final TransportVersion version = TransportVersion.readVersion(in);
        in.setTransportVersion(version);
        final Authentication authentication = new Authentication(in);
        final List<RoleDescriptorsBytes> roleDescriptorsBytesList = in.readImmutableList(RoleDescriptorsBytes::new);
        return new RemoteAccessAuthentication(authentication, roleDescriptorsBytesList);
    }

    public static final class RoleDescriptorsBytes extends AbstractBytesReference {
        private final BytesReference rawBytes;

        public RoleDescriptorsBytes(BytesReference rawBytes) {
            this.rawBytes = rawBytes;
        }

        public RoleDescriptorsBytes(StreamInput streamInput) throws IOException {
            this(streamInput.readBytesReference());
        }

        public static RoleDescriptorsBytes fromRoleDescriptors(final Set<RoleDescriptor> roleDescriptors) throws IOException {
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            for (RoleDescriptor roleDescriptor : roleDescriptors) {
                builder.field(roleDescriptor.getName(), roleDescriptor);
            }
            builder.endObject();
            return new RoleDescriptorsBytes(BytesReference.bytes(builder));
        }

        public Set<RoleDescriptor> toRoleDescriptors() {
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

        @Override
        public byte get(int index) {
            return rawBytes.get(index);
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
