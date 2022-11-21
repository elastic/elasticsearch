/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public record RoleDescriptorsIntersection(Collection<Set<RoleDescriptor>> roleDescriptorsSets) implements ToXContentObject, Writeable {
    public static final RoleDescriptorsIntersection EMPTY = new RoleDescriptorsIntersection(Set.of());

    public RoleDescriptorsIntersection(StreamInput in) throws IOException {
        this(in.readImmutableList(inner -> inner.readSet(RoleDescriptor::new)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(roleDescriptorsSets, StreamOutput::writeCollection);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (final Set<RoleDescriptor> roleDescriptorsSet : roleDescriptorsSets) {
            builder.startObject();
            for (final RoleDescriptor roleDescriptor : roleDescriptorsSet) {
                builder.field(roleDescriptor.getName(), roleDescriptor);
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    public static RoleDescriptorsIntersection fromXContent(XContentParser xContentParser) throws IOException {
        if (xContentParser.currentToken() == null) {
            xContentParser.nextToken();
        }
        final List<Set<RoleDescriptor>> roleDescriptorsSets = XContentParserUtils.parseList(xContentParser, p -> {
            XContentParser.Token token = p.currentToken();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, p);
            final List<RoleDescriptor> roleDescriptors = new ArrayList<>();
            while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, p);
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.nextToken(), p);
                roleDescriptors.add(RoleDescriptor.parse(p.currentName(), p, false));
            }
            return Set.copyOf(roleDescriptors);
        });
        return new RoleDescriptorsIntersection(List.copyOf(roleDescriptorsSets));
    }

    public BytesReference toBytesReference() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            this.writeTo(output);
            return output.bytes();
        }
    }

    public static RoleDescriptorsIntersection fromBytesReference(final BytesReference serialized) throws IOException {
        final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    ConfigurableClusterPrivilege.class,
                    ConfigurableClusterPrivileges.ManageApplicationPrivileges.WRITEABLE_NAME,
                    ConfigurableClusterPrivileges.ManageApplicationPrivileges::createFrom
                ),
                new NamedWriteableRegistry.Entry(
                    ConfigurableClusterPrivilege.class,
                    ConfigurableClusterPrivileges.WriteProfileDataPrivileges.WRITEABLE_NAME,
                    ConfigurableClusterPrivileges.WriteProfileDataPrivileges::createFrom
                )
            )
        );
        try (StreamInput input = new NamedWriteableAwareStreamInput(serialized.streamInput(), namedWriteableRegistry)) {
            return new RoleDescriptorsIntersection(input);
        }
    }

    public String toJson() throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        this.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return Strings.toString(builder);
    }

    public static RoleDescriptorsIntersection fromJson(final String jsonString) throws IOException {
        try (XContentParser p = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, jsonString)) {
            return RoleDescriptorsIntersection.fromXContent(p);
        }
    }
}
