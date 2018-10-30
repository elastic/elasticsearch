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

public class RoleDescriptor implements ToXContentObject {

    private final String name;
    private final Collection<ClusterPrivilege> clusterPrivileges;
    private final Collection<ManageApplicationPrivileges> manageApplicationPrivileges;
    private final Collection<IndicesPrivileges> indicesPrivileges;
    private final Collection<ApplicationResourcePrivileges> applicationResourcePrivileges;
    private final Collection<String> runAs;
    private final Map<String, Object> metadata;
    
    private RoleDescriptor(String name, Collection<ClusterPrivilege> clusterPrivileges,
            Collection<ManageApplicationPrivileges> manageApplicationPrivileges, Collection<IndicesPrivileges> indicesPrivileges,
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
        this.manageApplicationPrivileges = manageApplicationPrivileges != null ? Collections.unmodifiableCollection(manageApplicationPrivileges) : Collections.emptySet();
        // no indices privileges are granted unless otherwise specified
        this.indicesPrivileges = indicesPrivileges != null ? Collections.unmodifiableCollection(indicesPrivileges) : Collections.emptySet();
        // no application resource privileges are granted unless otherwise specified
        this.applicationResourcePrivileges = applicationResourcePrivileges != null ? Collections.unmodifiableCollection(applicationResourcePrivileges) : Collections.emptySet();
        // no run as privileges are granted unless otherwise specified
        this.runAs = runAs != null ? Collections.unmodifiableCollection(runAs) : Collections.emptySet();
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

}
