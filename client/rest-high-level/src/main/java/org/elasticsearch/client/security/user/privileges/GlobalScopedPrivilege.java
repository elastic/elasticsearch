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

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents generic global application privileges that can be scoped for each
 * application. The privilege definition is outside the Elasticsearch control.
 */
public class GlobalScopedPrivilege {

    private final String scope;
    private final Map<String, Object> privilege;

    /**
     * Constructs privilege over some "scope". The "scope" is usually an application
     * name but there is no constraint over this identifier.
     * 
     * @param scope The scope of the privilege.
     * @param privilege The privilege definition. This is out of the Elasticsearch control.
     */
    public GlobalScopedPrivilege(String scope, Map<String, Object> privilege) {
        this.scope = Objects.requireNonNull(scope);
        this.privilege = Collections.unmodifiableMap(Objects.requireNonNull(privilege));
    }

    public String getScope() {
        return scope;
    }

    public Map<String, Object> getRaw() {
        return privilege;
    }

    public static GlobalScopedPrivilege fromXContent(String scope, XContentParser parser) throws IOException {
        return new GlobalScopedPrivilege(scope, parser.map());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final GlobalScopedPrivilege that = (GlobalScopedPrivilege) o;
        return scope.equals(that.scope) && privilege.equals(that.privilege);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, privilege);
    }

}
