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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the privilege to "manage" certain applications. The "manage"
 * privilege is actually defined outside of Elasticsearch.
 */
public class ManageApplicationPrivilege extends GlobalScopedPrivilege {

    private static final String SCOPE = "manage";
    private static final String APPLICATIONS = "applications";

    public ManageApplicationPrivilege(Collection<String> applications) {
        super(SCOPE, Collections.singletonMap(APPLICATIONS, new HashSet<String>(Objects.requireNonNull(applications))));
        if (applications.isEmpty()) {
            throw new IllegalArgumentException("Applications cannot be empty. Simply don't add this privilege at all.");
        }
    }

    @SuppressWarnings("unchecked")
    public Set<String> getApplications() {
        return (Set<String>)getRaw().get(APPLICATIONS);
    }
}
