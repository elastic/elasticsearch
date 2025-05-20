/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class ProjectId implements Writeable, ToXContent {

    private static final String DEFAULT_STRING = "default";
    public static final ProjectId DEFAULT = new ProjectId(DEFAULT_STRING);
    public static final Reader<ProjectId> READER = ProjectId::readFrom;
    private static final int MAX_LENGTH = 128;

    private final String id;

    private ProjectId(String id) {
        if (Strings.isNullOrBlank(id)) {
            throw new IllegalArgumentException("project-id cannot be empty");
        }
        if (isValidFormatId(id) == false) {
            final var message = "project-id [" + id + "] must be alphanumeric ASCII with up to " + MAX_LENGTH + " chars";
            assert false : message;
            throw new IllegalArgumentException(message);
        }
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static ProjectId fromId(String id) {
        if (DEFAULT_STRING.equals(id)) {
            return DEFAULT;
        } else {
            return new ProjectId(id);
        }
    }

    public static boolean isValidFormatId(String id) {
        if (id.length() > MAX_LENGTH) {
            return false;
        }
        for (int i = 0; i < id.length(); i++) {
            char c = id.charAt(i);
            if (c > 0x7f) {
                return false;
            }
            if (isValidIdChar(c) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean isValidIdChar(char c) {
        // Allow '_' and '-' because they is used in based64 UUIDs which we often use in tests
        return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || c == '-';
    }

    public static ProjectId readFrom(StreamInput in) throws IOException {
        return fromId(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(id);
    }

    public static ProjectId fromXContent(XContentParser parser) throws IOException {
        return new ProjectId(parser.text());
    }

    public static ProjectId ofNullable(@Nullable String id, @Nullable ProjectId fallback) {
        return id == null ? fallback : new ProjectId(id);
    }

    @Override
    public String toString() {
        return this.id;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ProjectId projectId = (ProjectId) o;
        return Objects.equals(id, projectId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
