/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.SecurityExtension;
import org.elasticsearch.xpack.security.support.MetadataUtils;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.watch.Watch;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class WatcherSecurityExtension implements SecurityExtension {
    @Override
    public Map<String, RoleDescriptor> getReservedRoles() {
        Map<String, RoleDescriptor> roles = new HashMap<>();

        roles.put("watcher_admin",
                  new RoleDescriptor("watcher_admin",
                                     new String[] { "manage_watcher" },
                                     new RoleDescriptor.IndicesPrivileges[] {
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(Watch.INDEX,
                                                          TriggeredWatchStore.INDEX_NAME,
                                                          HistoryStore.INDEX_PREFIX + "*")
                                                 .privileges("read").build()
                                     },
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        roles.put("watcher_user",
                  new RoleDescriptor("watcher_user",
                                     new String[] { "monitor_watcher" },
                                     new RoleDescriptor.IndicesPrivileges[] {
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(Watch.INDEX)
                                                 .privileges("read")
                                                 .build(),
                                         RoleDescriptor.IndicesPrivileges.builder()
                                                 .indices(HistoryStore.INDEX_PREFIX + "*")
                                                 .privileges("read")
                                                 .build()
                                     },
                                     null,
                                     MetadataUtils.DEFAULT_RESERVED_METADATA));

        return Collections.unmodifiableMap(roles);
    }
}
