/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.permission;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.authz.privilege.ClusterPrivilege;
import org.elasticsearch.shield.authz.privilege.GeneralPrivilege;
import org.elasticsearch.shield.authz.privilege.IndexPrivilege;
import org.elasticsearch.shield.authz.privilege.Privilege;
import org.elasticsearch.shield.authz.privilege.Privilege.Name;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class Role extends GlobalPermission {

    private final String name;

    Role(String name, ClusterPermission.Core cluster, IndicesPermission.Core indices, RunAsPermission.Core runAs) {
        super(cluster, indices, runAs);
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public ClusterPermission.Core cluster() {
        return (ClusterPermission.Core) super.cluster();
    }

    @Override
    public IndicesPermission.Core indices() {
        return (IndicesPermission.Core) super.indices();
    }

    @Override
    public RunAsPermission.Core runAs() {
        return (RunAsPermission.Core) super.runAs();
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static Builder builder(RoleDescriptor rd) {
        return new Builder(rd);
    }

    public static class Builder {

        private final String name;
        private ClusterPermission.Core cluster = ClusterPermission.Core.NONE;
        private RunAsPermission.Core runAs = RunAsPermission.Core.NONE;
        private List<IndicesPermission.Group> groups = new ArrayList<>();

        private Builder(String name) {
            this.name = name;
        }

        private Builder(RoleDescriptor rd) {
            this.name = rd.getName();
            if (rd.getClusterPrivileges().length == 0) {
                cluster = ClusterPermission.Core.NONE;
            } else {
                this.cluster(ClusterPrivilege.get((new Privilege.Name(rd.getClusterPrivileges()))));
            }
            groups.addAll(convertFromIndicesPrivileges(rd.getIndicesPrivileges()));
            String[] rdRunAs = rd.getRunAs();
            if (rdRunAs != null && rdRunAs.length > 0) {
                this.runAs(new GeneralPrivilege(new Privilege.Name(rdRunAs), rdRunAs));
            }
        }

        // FIXME we should throw an exception if we have already set cluster or runAs...
        public Builder cluster(ClusterPrivilege privilege) {
            cluster = new ClusterPermission.Core(privilege);
            return this;
        }

        public Builder runAs(GeneralPrivilege privilege) {
            runAs = new RunAsPermission.Core(privilege);
            return this;
        }

        public Builder add(IndexPrivilege privilege, String... indices) {
            groups.add(new IndicesPermission.Group(privilege, null, null, indices));
            return this;
        }

        public Builder add(List<String> fields, BytesReference query, IndexPrivilege privilege, String... indices) {
            groups.add(new IndicesPermission.Group(privilege, fields, query, indices));
            return this;
        }

        public Role build() {
            IndicesPermission.Core indices = groups.isEmpty() ? IndicesPermission.Core.NONE :
                    new IndicesPermission.Core(groups.toArray(new IndicesPermission.Group[groups.size()]));
            return new Role(name, cluster, indices, runAs);
        }

        static List<IndicesPermission.Group> convertFromIndicesPrivileges(RoleDescriptor.IndicesPrivileges[] indicesPrivileges) {
            List<IndicesPermission.Group> list = new ArrayList<>(indicesPrivileges.length);
            for (RoleDescriptor.IndicesPrivileges privilege : indicesPrivileges) {
                list.add(new IndicesPermission.Group(IndexPrivilege.get(new Privilege.Name(privilege.getPrivileges())),
                        privilege.getFields() == null ? null : Arrays.asList(privilege.getFields()),
                        privilege.getQuery(),
                        privilege.getIndices()));

            }
            return list;
        }
    }
}
