/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Response for a {@link HasPrivilegesRequest}
 */
public class HasPrivilegesResponse extends ActionResponse implements ToXContentObject {
    private String username;
    private boolean completeMatch;
    private Map<String, Boolean> cluster;
    private Set<ResourcePrivileges> index;
    private Map<String, Set<ResourcePrivileges>> application;

    public HasPrivilegesResponse() {
        this("", true, Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    }

    public HasPrivilegesResponse(StreamInput in) throws IOException {
        super(in);
        completeMatch = in.readBoolean();
        cluster = in.readMap(StreamInput::readString, StreamInput::readBoolean);
        index = readResourcePrivileges(in);
        application = in.readMap(StreamInput::readString, HasPrivilegesResponse::readResourcePrivileges);
        username = in.readString();
    }

    public HasPrivilegesResponse(
        String username,
        boolean completeMatch,
        Map<String, Boolean> cluster,
        Collection<ResourcePrivileges> index,
        Map<String, Collection<ResourcePrivileges>> application
    ) {
        super();
        this.username = username;
        this.completeMatch = completeMatch;
        this.cluster = Collections.unmodifiableMap(cluster);
        this.index = Collections.unmodifiableSet(sorted(index));
        final Map<String, Set<ResourcePrivileges>> applicationPrivileges = new HashMap<>();
        application.forEach((key, val) -> applicationPrivileges.put(key, Collections.unmodifiableSet(sorted(val))));
        this.application = Collections.unmodifiableMap(applicationPrivileges);
    }

    private static Set<ResourcePrivileges> sorted(Collection<ResourcePrivileges> resources) {
        final Set<ResourcePrivileges> set = new TreeSet<>(Comparator.comparing(o -> o.getResource()));
        set.addAll(resources);
        return set;
    }

    public String getUsername() {
        return username;
    }

    public boolean isCompleteMatch() {
        return completeMatch;
    }

    public Map<String, Boolean> getClusterPrivileges() {
        return cluster;
    }

    public Set<ResourcePrivileges> getIndexPrivileges() {
        return index;
    }

    /**
     * Retrieves the results from checking application privileges,
     * @return A {@code Map} keyed by application-name
     */
    public Map<String, Set<ResourcePrivileges>> getApplicationPrivileges() {
        return application;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final HasPrivilegesResponse response = (HasPrivilegesResponse) o;
        return completeMatch == response.completeMatch
            && Objects.equals(username, response.username)
            && Objects.equals(cluster, response.cluster)
            && Objects.equals(index, response.index)
            && Objects.equals(application, response.application);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, completeMatch, cluster, index, application);
    }

    private static Set<ResourcePrivileges> readResourcePrivileges(StreamInput in) throws IOException {
        final int count = in.readVInt();
        final Set<ResourcePrivileges> set = new TreeSet<>(Comparator.comparing(o -> o.getResource()));
        for (int i = 0; i < count; i++) {
            final String index = in.readString();
            final Map<String, Boolean> privileges = in.readMap(StreamInput::readString, StreamInput::readBoolean);
            set.add(ResourcePrivileges.builder(index).addPrivileges(privileges).build());
        }
        return set;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(completeMatch);
        out.writeMap(cluster, StreamOutput::writeString, StreamOutput::writeBoolean);
        writeResourcePrivileges(out, index);
        out.writeMap(application, StreamOutput::writeString, HasPrivilegesResponse::writeResourcePrivileges);
        out.writeString(username);
    }

    private static void writeResourcePrivileges(StreamOutput out, Set<ResourcePrivileges> privileges) throws IOException {
        out.writeVInt(privileges.size());
        for (ResourcePrivileges priv : privileges) {
            out.writeString(priv.getResource());
            out.writeMap(priv.getPrivileges(), StreamOutput::writeString, StreamOutput::writeBoolean);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "{"
            + "username="
            + username
            + ","
            + "completeMatch="
            + completeMatch
            + ","
            + "cluster="
            + cluster
            + ","
            + "index="
            + index
            + ","
            + "application="
            + application
            + "}";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("username", username).field("has_all_requested", completeMatch);

        builder.field("cluster");
        builder.map(cluster);

        appendResources(builder, "index", index);

        builder.startObject("application");
        for (String app : application.keySet()) {
            appendResources(builder, app, application.get(app));
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    private static void appendResources(XContentBuilder builder, String field, Set<ResourcePrivileges> privileges) throws IOException {
        builder.startObject(field);
        for (ResourcePrivileges privilege : privileges) {
            builder.field(privilege.getResource());
            builder.map(privilege.getPrivileges());
        }
        builder.endObject();
    }

}
