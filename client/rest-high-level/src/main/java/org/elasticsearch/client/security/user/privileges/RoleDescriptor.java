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

public class RoleDescriptor implements ToXContentObject {

    private final String name;
    private final Collection<ClusterPrivilege> clusterPrivileges;
    private final Collection<ManageApplicationsPrivilege> manageApplicationPrivileges;
    private final Collection<IndicesPrivileges> indicesPrivileges;
    private final Collection<ApplicationResourcePrivileges> applicationResourcePrivileges;
    private final Collection<String> runAs;
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
        this.clusterPrivileges = clusterPrivileges != null ? Collections.unmodifiableCollection(clusterPrivileges) : Collections.emptySet();
        // no manage application privileges are granted unless otherwise specified
        this.manageApplicationPrivileges = manageApplicationPrivileges != null
                ? Collections.unmodifiableCollection(manageApplicationPrivileges)
                : Collections.emptySet();
        // no indices privileges are granted unless otherwise specified
        this.indicesPrivileges = indicesPrivileges != null ? Collections.unmodifiableCollection(indicesPrivileges) : Collections.emptySet();
        // no application resource privileges are granted unless otherwise specified
        this.applicationResourcePrivileges = applicationResourcePrivileges != null
                ? Collections.unmodifiableCollection(applicationResourcePrivileges)
                : Collections.emptySet();
        // no run as privileges are granted unless otherwise specified
        this.runAs = runAs != null ? Collections.unmodifiableCollection(runAs) : Collections.emptySet();
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
    }

    public String getName() {
        return name;
    }

    public Collection<ClusterPrivilege> getClusterPrivileges() {
        return clusterPrivileges;
    }

    public Collection<ManageApplicationsPrivilege> getManageApplicationPrivileges() {
        return manageApplicationPrivileges;
    }

    public Collection<IndicesPrivileges> getIndicesPrivileges() {
        return indicesPrivileges;
    }

    public Collection<ApplicationResourcePrivileges> getApplicationResourcePrivileges() {
        return applicationResourcePrivileges;
    }

    public Collection<String> getRunAs() {
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
