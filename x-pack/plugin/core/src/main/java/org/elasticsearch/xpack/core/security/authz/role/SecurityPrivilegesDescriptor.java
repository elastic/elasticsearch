/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.role;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class SecurityPrivilegesDescriptor implements ToXContentObject, Writeable {
    public static final SecurityPrivilegesDescriptor EMPTY = new SecurityPrivilegesDescriptor();

    public static SecurityPrivilegesDescriptor createFrom(StreamInput in) {
        return EMPTY;
    }

    public static SecurityPrivilegesDescriptor parse(String roleName, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse security privileges for role [{}]. expected field [{}] value " +
                "to be an array, but found [{}] instead", roleName, parser.currentName(), parser.currentToken());
        }
        parser.map(); // ignore for now
        return EMPTY;
    }

    public boolean isEmpty() {
        return true;
    }

    @Override
    public String toString() {
        return "{}";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // nothing
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }
}
