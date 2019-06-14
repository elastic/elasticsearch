/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;

/**
 * An {@code ApplicationPrivilegeDescriptor} is a representation of a <em>stored</em> {@link ApplicationPrivilege}.
 * A user (via a role) can be granted an application privilege by name (e.g. ("myapp", "read").
 * In general, this privilege name will correspond to a pre-defined {@link ApplicationPrivilegeDescriptor}, which then
 * is used to determine the set of actions granted by the privilege.
 */
public class ApplicationPrivilegeDescriptor implements ToXContentObject, Writeable {

    public static final String DOC_TYPE_VALUE = "application-privilege";

    private static final ObjectParser<Builder, Boolean> PARSER = new ObjectParser<>(DOC_TYPE_VALUE, Builder::new);

    static {
        PARSER.declareString(Builder::applicationName, Fields.APPLICATION);
        PARSER.declareString(Builder::privilegeName, Fields.NAME);
        PARSER.declareStringArray(Builder::actions, Fields.ACTIONS);
        PARSER.declareObject(Builder::metadata, (parser, context) -> parser.map(), Fields.METADATA);
        PARSER.declareField((parser, builder, allowType) -> builder.type(parser.text(), allowType), Fields.TYPE,
            ObjectParser.ValueType.STRING);
    }

    private String application;
    private String name;
    private Set<String> actions;
    private Map<String, Object> metadata;

    public ApplicationPrivilegeDescriptor(String application, String name, Set<String> actions, Map<String, Object> metadata) {
        this.application = Objects.requireNonNull(application);
        this.name = Objects.requireNonNull(name);
        this.actions = Collections.unmodifiableSet(actions);
        this.metadata = Collections.unmodifiableMap(metadata);
    }

    public ApplicationPrivilegeDescriptor(StreamInput input) throws IOException {
        this.application = input.readString();
        this.name = input.readString();
        this.actions = Collections.unmodifiableSet(input.readSet(StreamInput::readString));
        this.metadata = Collections.unmodifiableMap(input.readMap());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(application);
        out.writeString(name);
        out.writeCollection(actions, StreamOutput::writeString);
        out.writeMap(metadata);
    }

    public String getApplication() {
        return application;
    }

    public String getName() {
        return name;
    }

    public Set<String> getActions() {
        return actions;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, false);
    }

    public XContentBuilder toXContent(XContentBuilder builder, boolean includeTypeField) throws IOException {
        builder.startObject()
            .field(Fields.APPLICATION.getPreferredName(), application)
            .field(Fields.NAME.getPreferredName(), name)
            .field(Fields.ACTIONS.getPreferredName(), actions)
            .field(Fields.METADATA.getPreferredName(), metadata);
        if (includeTypeField) {
            builder.field(Fields.TYPE.getPreferredName(), DOC_TYPE_VALUE);
        }
        return builder.endObject();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{[" + application + "],[" + name + "],[" + collectionToCommaDelimitedString(actions) + "]}";
    }

    /**
     * Construct a new {@link ApplicationPrivilegeDescriptor} from XContent.
     *
     * @param defaultApplication The application name to use if none is specified in the XContent body
     * @param defaultName The privilege name to use if none is specified in the XContent body
     * @param allowType If true, accept a "type" field (for which the value must match {@link #DOC_TYPE_VALUE});
     */
    public static ApplicationPrivilegeDescriptor parse(XContentParser parser, String defaultApplication, String defaultName,
                                             boolean allowType) throws IOException {
        final Builder builder = PARSER.parse(parser, allowType);
        if (builder.applicationName == null) {
            builder.applicationName(defaultApplication);
        }
        if (builder.privilegeName == null) {
            builder.privilegeName(defaultName);
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ApplicationPrivilegeDescriptor that = (ApplicationPrivilegeDescriptor) o;
        return Objects.equals(this.application, that.application) &&
            Objects.equals(this.name, that.name) &&
            Objects.equals(this.actions, that.actions) &&
            Objects.equals(this.metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(application, name, actions, metadata);
    }

    private static final class Builder {
        private String applicationName;
        private String privilegeName;
        private Set<String> actions = Collections.emptySet();
        private Map<String, Object> metadata = Collections.emptyMap();

        private Builder applicationName(String applicationName) {
            this.applicationName = applicationName;
            return this;
        }

        private Builder privilegeName(String privilegeName) {
            this.privilegeName = privilegeName;
            return this;
        }

        private Builder actions(Collection<String> actions) {
            this.actions = new LinkedHashSet<>(actions);
            return this;
        }

        private Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        private Builder type(String type, boolean allowed) {
            if (allowed == false) {
                throw new IllegalStateException("Field " + Fields.TYPE.getPreferredName() + " cannot be specified here");
            }
            if (ApplicationPrivilegeDescriptor.DOC_TYPE_VALUE.equals(type) == false) {
                throw new IllegalStateException("XContent has wrong " + Fields.TYPE.getPreferredName() + " field " + type);
            }
            return this;
        }

        private ApplicationPrivilegeDescriptor build() {
            return new ApplicationPrivilegeDescriptor(applicationName, privilegeName, actions, metadata);
        }
    }

    public interface Fields {
        ParseField APPLICATION = new ParseField("application");
        ParseField NAME = new ParseField("name");
        ParseField ACTIONS = new ParseField("actions");
        ParseField METADATA = new ParseField("metadata");
        ParseField TYPE = new ParseField("type");
    }
}
