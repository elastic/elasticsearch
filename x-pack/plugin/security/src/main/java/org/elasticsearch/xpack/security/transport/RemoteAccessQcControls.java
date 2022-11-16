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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Serializable record of QC Authentication and Authorizations.
 * It is transmitted from a QC to an FC.
 * @param authentication QC Authentication instance of a successfully authenticated User or API Key.
 * @param roleDescriptorsBytesIntersection QC Authorizations instance for the QC Authenticated User or API Key.
 */
public record RemoteAccessQcControls(Authentication authentication, Collection<BytesReference> roleDescriptorsBytesIntersection) {

    public static RemoteAccessQcControls readFromContext(final ThreadContext ctx) throws IOException {
        final String header = ctx.getHeader(AuthenticationField.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY);
        return Strings.isEmpty(header) ? null : decode(header);
    }

    public static void writeToContext(
        final ThreadContext ctx,
        final Authentication authentication,
        final RoleDescriptorsIntersection roleDescriptorsIntersection
    ) throws IOException {
        ctx.putHeader(AuthenticationField.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY, encode(authentication, roleDescriptorsIntersection));
    }

    static String encode(final Authentication authentication, final RoleDescriptorsIntersection roleDescriptorsIntersection)
        throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(authentication.getEffectiveSubject().getVersion());
        Version.writeVersion(authentication.getEffectiveSubject().getVersion(), out);
        authentication.writeTo(out);
        out.writeVInt(roleDescriptorsIntersection.roleDescriptorsList().size());
        for (var roleDescriptors : roleDescriptorsIntersection.roleDescriptorsList()) {
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            for (RoleDescriptor roleDescriptor : roleDescriptors) {
                builder.field(roleDescriptor.getName(), roleDescriptor);
            }
            builder.endObject();
            out.writeBytesReference(BytesReference.bytes(builder));
        }
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
    }

    static RemoteAccessQcControls decode(final String header) throws IOException {
        final byte[] bytes = Base64.getDecoder().decode(header);
        final StreamInput in = StreamInput.wrap(bytes);
        final Version version = Version.readVersion(in);
        in.setVersion(version);
        final Authentication authentication = new Authentication(in);
        final Collection<BytesReference> roleDescriptorsBytesIntersection = new ArrayList<>();
        final int outerCount = in.readVInt();
        for (int i = 0; i < outerCount; i++) {
            roleDescriptorsBytesIntersection.add(in.readBytesReference());
        }
        return new RemoteAccessQcControls(authentication, roleDescriptorsBytesIntersection);
    }

    public static Set<RoleDescriptor> parseRoleDescriptorsBytes(BytesReference bytesReference) {
        if (bytesReference == null) {
            return Collections.emptySet();
        }
        final LinkedHashSet<RoleDescriptor> roleDescriptors = new LinkedHashSet<>();
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, bytesReference, XContentType.JSON)) {
            parser.nextToken(); // skip outer start object
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                parser.nextToken(); // role name
                String roleName = parser.currentName();
                roleDescriptors.add(RoleDescriptor.parse(roleName, parser, false));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return roleDescriptors;
    }
}
