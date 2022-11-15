/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public record RemoteAccessAuthentication(Authentication authentication, Collection<Set<BytesReference>> roleDescriptorsBytesList) {

    private static final String REMOTE_ACCESS_AUTHENTICATION_HEADER = "_remote_access_authentication";

    public static void writeToContext(
        final ThreadContext ctx,
        final Authentication authentication,
        final RoleDescriptorsIntersection roleDescriptorsIntersection
    ) throws IOException {
        ctx.putHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER, encode(authentication, roleDescriptorsIntersection));
    }

    static String encode(Authentication authentication, RoleDescriptorsIntersection roleDescriptorsIntersection) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(authentication.getEffectiveSubject().getVersion());
        Version.writeVersion(authentication.getEffectiveSubject().getVersion(), out);
        authentication.writeTo(out);
        out.writeVInt(roleDescriptorsIntersection.roleDescriptorsList().size());
        for (var roleDescriptors : roleDescriptorsIntersection.roleDescriptorsList()) {
            out.writeVInt(roleDescriptors.size());
            for (var roleDescriptor : roleDescriptors) {
                final BytesStreamOutput rdOut = new BytesStreamOutput();
                roleDescriptor.writeTo(rdOut);
                out.writeBytesReference(rdOut.bytes());
            }
        }
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
    }

    public static RemoteAccessAuthentication readFromContext(final ThreadContext ctx) throws IOException {
        return decode(ctx.getHeader(REMOTE_ACCESS_AUTHENTICATION_HEADER));
    }

    static RemoteAccessAuthentication decode(String header) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(header);
        StreamInput in = StreamInput.wrap(bytes);
        Version version = Version.readVersion(in);
        in.setVersion(version);

        final Authentication authentication = new Authentication(in);
        final int outerCount = in.readVInt();
        final Collection<Set<BytesReference>> roleDescriptorsBytesIntersection = new ArrayList<>();
        for (int i = 0; i < outerCount; i++) {
            final int innerCount = in.readVInt();
            Set<BytesReference> rds = new HashSet<>();
            for (int j = 0; j < innerCount; j++) {
                rds.add(in.readBytesReference());
            }
            roleDescriptorsBytesIntersection.add(rds);
        }

        return new RemoteAccessAuthentication(authentication, roleDescriptorsBytesIntersection);
    }
}
