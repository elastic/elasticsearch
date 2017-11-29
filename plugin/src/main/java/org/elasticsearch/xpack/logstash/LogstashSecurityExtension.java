/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.logstash;

import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.SecurityExtension;
import org.elasticsearch.xpack.security.support.MetadataUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LogstashSecurityExtension implements SecurityExtension {
    @Override
    public Map<String, RoleDescriptor> getReservedRoles() {
        Map<String, RoleDescriptor> roles = new HashMap<>();

        roles.put("logstash_admin",
                  new RoleDescriptor("logstash_admin",
                                     null,
                                     new RoleDescriptor.IndicesPrivileges[]{
                                         RoleDescriptor.IndicesPrivileges.builder().indices(".logstash*")
                                                 .privileges("create", "delete", "index", "manage", "read")
                                                 .build()
                                     },
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        return Collections.unmodifiableMap(roles);
    }
}
