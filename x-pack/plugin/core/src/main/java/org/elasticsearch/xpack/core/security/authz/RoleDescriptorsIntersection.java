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
import java.util.List;

public record RoleDescriptorsIntersection(List<List<RoleDescriptor>> roleDescriptorsList) implements ToXContentObject, Writeable {

    public RoleDescriptorsIntersection(StreamInput in) throws IOException {
        this(RoleDescriptorsIntersection.readRoleDescriptorsListFrom(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(roleDescriptorsList.size());
        for (List<RoleDescriptor> roleDescriptors : roleDescriptorsList) {
            out.writeList(roleDescriptors);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        {
            for (List<RoleDescriptor> roleDescriptors : roleDescriptorsList) {
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

    public static RoleDescriptorsIntersection fromXContent(XContentParser p) throws IOException {
        XContentParser.Token token = p.currentToken();
        if (token == null) {
            token = p.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, token, p);
        final List<List<RoleDescriptor>> roleDescriptorsList = new ArrayList<>();
        while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, p);
            final List<RoleDescriptor> roleDescriptors = new ArrayList<>();
            while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, p);
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.nextToken(), p);
                roleDescriptors.add(RoleDescriptor.parse(p.currentName(), p, false));
            }
            roleDescriptorsList.add(List.copyOf(roleDescriptors));
        }
        return new RoleDescriptorsIntersection(List.copyOf(roleDescriptorsList));
    }

    private static List<List<RoleDescriptor>> readRoleDescriptorsListFrom(StreamInput in) throws IOException {
        final List<List<RoleDescriptor>> roleDescriptorsList = new ArrayList<>();
        final int size = in.readVInt();
        assert size >= 0;
        if (size < 0) {
            throw new IllegalArgumentException("list of role descriptors size must be greater than 0");
        }
        for (int i = 0; i < size; i++) {
            roleDescriptorsList.add(List.copyOf(in.readList(RoleDescriptor::new)));
        }
        return List.copyOf(roleDescriptorsList);
    }
}
