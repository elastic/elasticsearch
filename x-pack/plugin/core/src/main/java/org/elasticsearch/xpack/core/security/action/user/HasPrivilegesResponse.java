/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

/**
 * Response for a {@link HasPrivilegesRequest}
 */
public class HasPrivilegesResponse extends ActionResponse {
    private boolean completeMatch;
    private Map<String, Boolean> cluster;
    private List<IndexPrivileges> index;

    public HasPrivilegesResponse() {
        this(true, Collections.emptyMap(), Collections.emptyList());
    }

    public HasPrivilegesResponse(boolean completeMatch, Map<String, Boolean> cluster, Collection<IndexPrivileges> index) {
        super();
        this.completeMatch = completeMatch;
        this.cluster = new HashMap<>(cluster);
        this.index = new ArrayList<>(index);
    }

    public boolean isCompleteMatch() {
        return completeMatch;
    }

    public Map<String, Boolean> getClusterPrivileges() {
        return Collections.unmodifiableMap(cluster);
    }

    public List<IndexPrivileges> getIndexPrivileges() {
        return Collections.unmodifiableList(index);
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        completeMatch = in.readBoolean();
        int count = in.readVInt();
        index = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final String index = in.readString();
            final Map<String, Boolean> privileges = in.readMap(StreamInput::readString, StreamInput::readBoolean);
            this.index.add(new IndexPrivileges(index, privileges));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(completeMatch);
        out.writeVInt(index.size());
        for (IndexPrivileges index : index) {
            out.writeString(index.index);
            out.writeMap(index.privileges, StreamOutput::writeString, StreamOutput::writeBoolean);
        }
    }

    public static class IndexPrivileges {
        private final String index;
        private final Map<String, Boolean> privileges;

        public IndexPrivileges(String index, Map<String, Boolean> privileges) {
            this.index = Objects.requireNonNull(index);
            this.privileges = Collections.unmodifiableMap(privileges);
        }

        public String getIndex() {
            return index;
        }

        public Map<String, Boolean> getPrivileges() {
            return privileges;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "index='" + index + '\'' +
                    ", privileges=" + privileges +
                    '}';
        }

        @Override
        public int hashCode() {
            int result = index.hashCode();
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

            final IndexPrivileges other = (IndexPrivileges) o;
            return this.index.equals(other.index) && this.privileges.equals(other.privileges);
        }
    }
}
