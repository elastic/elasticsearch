/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public record RoleDescriptorsIntersection(Collection<Set<RoleDescriptor>> roleDescriptorsList) implements ToXContentObject, Writeable {

    public static final RoleDescriptorsIntersection EMPTY = new RoleDescriptorsIntersection(Collections.emptyList());

    private static final RoleDescriptor.Parser ROLE_DESCRIPTOR_PARSER = RoleDescriptor.parserBuilder()
        .allowRestriction(true)
        .allowDescription(true)
        .build();

    public RoleDescriptorsIntersection(RoleDescriptor roleDescriptor) {
        this(List.of(Set.of(roleDescriptor)));
    }

    public RoleDescriptorsIntersection(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(inner -> inner.readCollectionAsSet(RoleDescriptor::new)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(roleDescriptorsList, StreamOutput::writeCollection);
    }

    public boolean isEmpty() {
        return roleDescriptorsList().isEmpty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        {
            for (Set<RoleDescriptor> roleDescriptors : roleDescriptorsList) {
                builder.startObject();
                for (RoleDescriptor roleDescriptor : roleDescriptors) {
                    builder.field(roleDescriptor.getName(), roleDescriptor);
                }
                builder.endObject();
            }
        }
        builder.endArray();
        return builder;
    }

    public static RoleDescriptorsIntersection fromXContent(XContentParser xContentParser) throws IOException {
        if (xContentParser.currentToken() == null) {
            xContentParser.nextToken();
        }
        final List<Set<RoleDescriptor>> roleDescriptorsList = XContentParserUtils.parseList(xContentParser, p -> {
            XContentParser.Token token = p.currentToken();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, p);
            final List<RoleDescriptor> roleDescriptors = new ArrayList<>();
            while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, p);
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.nextToken(), p);
                roleDescriptors.add(ROLE_DESCRIPTOR_PARSER.parse(p.currentName(), p));
            }
            return Set.copyOf(roleDescriptors);
        });
        return new RoleDescriptorsIntersection(List.copyOf(roleDescriptorsList));
    }
}
