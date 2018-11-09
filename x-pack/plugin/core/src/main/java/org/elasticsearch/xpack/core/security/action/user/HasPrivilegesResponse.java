/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Response for a {@link HasPrivilegesRequest}
 */
public class HasPrivilegesResponse extends ActionResponse {
    private boolean completeMatch;
    private Map<String, Boolean> cluster;
    private List<ResourcePrivileges> index;
    private Map<String, List<ResourcePrivileges>> application;

    public HasPrivilegesResponse() {
        this(true, Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    }

    public HasPrivilegesResponse(boolean completeMatch, Map<String, Boolean> cluster, Collection<ResourcePrivileges> index,
                                 Map<String, Collection<ResourcePrivileges>> application) {
        super();
        this.completeMatch = completeMatch;
        this.cluster = new HashMap<>(cluster);
        this.index = new ArrayList<>(index);
        this.application = new HashMap<>();
        application.forEach((key, val) -> this.application.put(key, Collections.unmodifiableList(new ArrayList<>(val))));
    }

    public boolean isCompleteMatch() {
        return completeMatch;
    }

    public Map<String, Boolean> getClusterPrivileges() {
        return Collections.unmodifiableMap(cluster);
    }

    public List<ResourcePrivileges> getIndexPrivileges() {
        return Collections.unmodifiableList(index);
    }

    /**
     * Retrieves the results from checking application privileges,
     * @return A {@code Map} keyed by application-name
     */
    public Map<String, List<ResourcePrivileges>> getApplicationPrivileges() {
        return Collections.unmodifiableMap(application);
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        completeMatch = in.readBoolean();
        index = readResourcePrivileges(in);
        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            application = in.readMap(StreamInput::readString, HasPrivilegesResponse::readResourcePrivileges);
        }
    }

    private static List<ResourcePrivileges> readResourcePrivileges(StreamInput in) throws IOException {
        final int count = in.readVInt();
        final List<ResourcePrivileges> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final String index = in.readString();
            final Map<String, Boolean> privileges = in.readMap(StreamInput::readString, StreamInput::readBoolean);
            list.add(new ResourcePrivileges(index, privileges));
        }
        return list;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(completeMatch);
        writeResourcePrivileges(out, index);
        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeMap(application, StreamOutput::writeString, HasPrivilegesResponse::writeResourcePrivileges);
        }
    }

    private static void writeResourcePrivileges(StreamOutput out, List<ResourcePrivileges> privileges) throws IOException {
        out.writeVInt(privileges.size());
        for (ResourcePrivileges priv : privileges) {
            out.writeString(priv.resource);
            out.writeMap(priv.privileges, StreamOutput::writeString, StreamOutput::writeBoolean);
        }
    }

    public static class ResourcePrivileges {
        private final String resource;
        private final Map<String, Boolean> privileges;

        public ResourcePrivileges(String resource, Map<String, Boolean> privileges) {
            this.resource = Objects.requireNonNull(resource);
            this.privileges = Collections.unmodifiableMap(privileges);
        }

        public String getResource() {
            return resource;
        }

        public Map<String, Boolean> getPrivileges() {
            return privileges;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "resource='" + resource + '\'' +
                    ", privileges=" + privileges +
                    '}';
        }

        @Override
        public int hashCode() {
            int result = resource.hashCode();
            result = 31 * result + privileges.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final ResourcePrivileges other = (ResourcePrivileges) o;
            return this.resource.equals(other.resource) && this.privileges.equals(other.privileges);
        }
    }
}
