/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.permission.IngestAdminRole;
import org.elasticsearch.xpack.security.authz.permission.KibanaRole;
import org.elasticsearch.xpack.security.authz.permission.KibanaUserRole;
import org.elasticsearch.xpack.security.authz.permission.LogstashSystemRole;
import org.elasticsearch.xpack.security.authz.permission.MonitoringUserRole;
import org.elasticsearch.xpack.security.authz.permission.RemoteMonitoringAgentRole;
import org.elasticsearch.xpack.security.authz.permission.ReportingUserRole;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.permission.SuperuserRole;
import org.elasticsearch.xpack.security.authz.permission.TransportClientRole;
import org.elasticsearch.xpack.security.user.SystemUser;

public class ReservedRolesStore {

    public ReservedRolesStore() {
    }

    public Role role(String role) {
        switch (role) {
            case SuperuserRole.NAME:
                return SuperuserRole.INSTANCE;
            case TransportClientRole.NAME:
                return TransportClientRole.INSTANCE;
            case KibanaUserRole.NAME:
                return KibanaUserRole.INSTANCE;
            case MonitoringUserRole.NAME:
                return MonitoringUserRole.INSTANCE;
            case RemoteMonitoringAgentRole.NAME:
                return RemoteMonitoringAgentRole.INSTANCE;
            case IngestAdminRole.NAME:
                return IngestAdminRole.INSTANCE;
            case ReportingUserRole.NAME:
                return ReportingUserRole.INSTANCE;
            case KibanaRole.NAME:
                return KibanaRole.INSTANCE;
            case LogstashSystemRole.NAME:
                return LogstashSystemRole.INSTANCE;
            default:
                return null;
        }
    }

    public Map<String, Object> usageStats() {
        return Collections.emptyMap();
    }

    public RoleDescriptor roleDescriptor(String role) {
        switch (role) {
            case SuperuserRole.NAME:
                return SuperuserRole.DESCRIPTOR;
            case TransportClientRole.NAME:
                return TransportClientRole.DESCRIPTOR;
            case KibanaUserRole.NAME:
                return KibanaUserRole.DESCRIPTOR;
            case MonitoringUserRole.NAME:
                return MonitoringUserRole.DESCRIPTOR;
            case RemoteMonitoringAgentRole.NAME:
                return RemoteMonitoringAgentRole.DESCRIPTOR;
            case IngestAdminRole.NAME:
                return IngestAdminRole.DESCRIPTOR;
            case ReportingUserRole.NAME:
                return ReportingUserRole.DESCRIPTOR;
            case KibanaRole.NAME:
                return KibanaRole.DESCRIPTOR;
            case LogstashSystemRole.NAME:
                return LogstashSystemRole.DESCRIPTOR;
            default:
                return null;
        }
    }

    public Collection<RoleDescriptor> roleDescriptors() {
        return Arrays.asList(SuperuserRole.DESCRIPTOR, TransportClientRole.DESCRIPTOR, KibanaUserRole.DESCRIPTOR,
                KibanaRole.DESCRIPTOR, MonitoringUserRole.DESCRIPTOR, RemoteMonitoringAgentRole.DESCRIPTOR,
                IngestAdminRole.DESCRIPTOR, ReportingUserRole.DESCRIPTOR, LogstashSystemRole.DESCRIPTOR);
    }

    public static Set<String> names() {
        return Sets.newHashSet(SuperuserRole.NAME, KibanaRole.NAME, TransportClientRole.NAME, KibanaUserRole.NAME,
                MonitoringUserRole.NAME, RemoteMonitoringAgentRole.NAME, IngestAdminRole.NAME, ReportingUserRole.NAME,
                LogstashSystemRole.NAME);
    }

    public static boolean isReserved(String role) {
        switch (role) {
            case SuperuserRole.NAME:
            case KibanaRole.NAME:
            case KibanaUserRole.NAME:
            case TransportClientRole.NAME:
            case MonitoringUserRole.NAME:
            case RemoteMonitoringAgentRole.NAME:
            case SystemUser.ROLE_NAME:
            case IngestAdminRole.NAME:
            case ReportingUserRole.NAME:
            case LogstashSystemRole.NAME:
                return true;
            default:
                return false;
        }
    }

}
