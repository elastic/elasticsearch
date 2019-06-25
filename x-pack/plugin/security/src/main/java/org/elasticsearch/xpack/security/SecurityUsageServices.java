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

package org.elasticsearch.xpack.security;

import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

/**
 * A wrapper around the services needed to produce usage information for the security feature.
 *
 * This class is temporary until actions can be constructed directly by plugins.
 */
class SecurityUsageServices {
    final Realms realms;
    final CompositeRolesStore rolesStore;
    final NativeRoleMappingStore roleMappingStore;
    final IPFilter ipFilter;

    SecurityUsageServices(Realms realms, CompositeRolesStore rolesStore, NativeRoleMappingStore roleMappingStore, IPFilter ipFilter) {
        this.realms = realms;
        this.rolesStore = rolesStore;
        this.roleMappingStore = roleMappingStore;
        this.ipFilter = ipFilter;
    }
}
