/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security.user.privileges;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class RoleDescriptor implements ToXContentObject {

    private final String name;
    // uniqueness and order are important for equals and hashcode
    private final SortedSet<ClusterPrivilege> clusterPrivileges;
    private final SortedSet<ManageApplicationsPrivilege> manageApplicationPrivileges;
    private final SortedSet<IndicesPrivileges> indicesPrivileges;
    private final SortedSet<ApplicationResourcePrivileges> applicationResourcePrivileges;
    private final SortedSet<String> runAs;
    private final Map<String, Object> metadata;

    private RoleDescriptor(String name, Collection<ClusterPrivilege> clusterPrivileges,
            Collection<ManageApplicationsPrivilege> manageApplicationPrivileges, Collection<IndicesPrivileges> indicesPrivileges,
            Collection<ApplicationResourcePrivileges> applicationResourcePrivileges, Collection<String> runAs,
            Map<String, Object> metadata) {
        // we do all null checks inside the constructor
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("role descriptor must have a name");
        }
        this.name = name;
        // no cluster privileges are granted unless otherwise specified
        this.clusterPrivileges = Collections
                .unmodifiableSortedSet(new TreeSet<>(clusterPrivileges != null ? clusterPrivileges : Collections.emptyList()));
        // no manage application privileges are granted unless otherwise specified
        this.manageApplicationPrivileges = Collections.unmodifiableSortedSet(
                new TreeSet<>(manageApplicationPrivileges != null ? manageApplicationPrivileges : Collections.emptyList()));
        // no indices privileges are granted unless otherwise specified
        this.indicesPrivileges = Collections
                .unmodifiableSortedSet(new TreeSet<>(indicesPrivileges != null ? indicesPrivileges : Collections.emptyList()));
        // no application resource privileges are granted unless otherwise specified
        this.applicationResourcePrivileges = Collections.unmodifiableSortedSet(
                new TreeSet<>(applicationResourcePrivileges != null ? applicationResourcePrivileges : Collections.emptyList()));
        // no run as privileges are granted unless otherwise specified
        this.runAs = Collections.unmodifiableSortedSet(new TreeSet<>(runAs != null ? runAs : Collections.emptyList()));
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
    }

    public String getName() {
        return name;
    }

    public Set<ClusterPrivilege> getClusterPrivileges() {
        return clusterPrivileges;
    }

    public Set<ManageApplicationsPrivilege> getManageApplicationPrivileges() {
        return manageApplicationPrivileges;
    }

    public Set<IndicesPrivileges> getIndicesPrivileges() {
        return indicesPrivileges;
    }

    public Set<ApplicationResourcePrivileges> getApplicationResourcePrivileges() {
        return applicationResourcePrivileges;
    }

    public SortedSet<String> getRunAs() {
        return runAs;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RoleDescriptor that = (RoleDescriptor) o;
        return name.equals(that.name) && clusterPrivileges.equals(that.clusterPrivileges)
                && manageApplicationPrivileges.equals(that.manageApplicationPrivileges) && indicesPrivileges.equals(that.indicesPrivileges)
                && applicationResourcePrivileges.equals(that.applicationResourcePrivileges) && runAs.equals(that.runAs)
                && metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, clusterPrivileges, manageApplicationPrivileges, indicesPrivileges, applicationResourcePrivileges, runAs,
                metadata);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("[");
        sb.append("name=").append(name);
        sb.append(", cluster=[").append(Strings.collectionToCommaDelimitedString(clusterPrivileges));
        sb.append("], global=[").append(Strings.collectionToCommaDelimitedString(manageApplicationPrivileges));
        sb.append("], indicesPrivileges=[").append(Strings.collectionToCommaDelimitedString(indicesPrivileges));
        sb.append("], applicationResourcePrivileges=[").append(Strings.collectionToCommaDelimitedString(applicationResourcePrivileges));
        sb.append("], runAs=[").append(Strings.collectionToCommaDelimitedString(runAs));
        sb.append("], metadata=[").append(metadata);
        sb.append("]]");
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

}
