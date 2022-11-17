/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Record of QC Subject's authentication and authorization instances.
 * @param authentication QC Subject's authentication instance
 * @param authorization QC Subject's authorization instance
 */
public record RemoteClusterSecuritySubjectAccess(Authentication authentication, Collection<BytesReference> authorization) {

    public static RemoteClusterSecuritySubjectAccess readFromThreadContextHeader(final ThreadContext ctx) throws IOException {
        final String headerValue = ctx.getHeader(AuthenticationField.RCS_SUBJECT_ACCESS_HEADER_KEY);
        return Strings.isEmpty(headerValue) ? null : decode(headerValue);
    }

    public static void writeToContext(
        final ThreadContext ctx,
        final Authentication authentication,
        final RoleDescriptorsIntersection authorization
    ) throws IOException {
        ctx.putHeader(AuthenticationField.RCS_SUBJECT_ACCESS_HEADER_KEY, encode(authentication, authorization));
    }

    static String encode(final Authentication authentication, final RoleDescriptorsIntersection authorization) throws IOException {
        final Version version = authentication.getEffectiveSubject().getVersion();
        final BytesStreamOutput out = new BytesStreamOutput();
        Version.writeVersion(version, out);
        out.setVersion(version);
        authentication.writeTo(out);
        authorization.writeTo(out);
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
    }

    static RemoteClusterSecuritySubjectAccess decode(final String headerValue) throws IOException {
        final byte[] bytes = Base64.getDecoder().decode(headerValue);
        final StreamInput in = StreamInput.wrap(bytes);
        final Version version = Version.readVersion(in);
        in.setVersion(version);
        final Authentication authentication = new Authentication(in);
        final Collection<BytesReference> roleDescriptorsBytesIntersection = new ArrayList<>();
        final int outerCount = in.readVInt();
        for (int i = 0; i < outerCount; i++) {
            roleDescriptorsBytesIntersection.add(in.readBytesReference());
        }
        return new RemoteClusterSecuritySubjectAccess(authentication, roleDescriptorsBytesIntersection);
    }

    static Set<RoleDescriptor> parseRoleDescriptorBytes(final BytesReference roleDescriptorBytes) {
        try (
            XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, roleDescriptorBytes, XContentType.JSON)
        ) {
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
}
