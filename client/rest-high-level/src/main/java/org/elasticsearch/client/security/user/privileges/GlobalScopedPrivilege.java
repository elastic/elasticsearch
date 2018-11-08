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
 * application. The privilege definition, as well as the scope identifier, are
 * outside of the Elasticsearch jurisdiction.
 */
public class GlobalScopedPrivilege {

    private final String scope;
    private final Map<String, Object> privilege;

    /**
     * Constructs privileges under some "scope". The "scope" is commonly an
     * "application" name but there is really no constraint over this identifier
     * from Elasticsearch's POV. The privilege definition is also out of
     * Elasticsearch's control.
     * 
     * @param scope
     *            The scope of the privilege.
     * @param privilege
     *            The privilege definition. This is out of the Elasticsearch's
     *            control.
     */
    public GlobalScopedPrivilege(String scope, Map<String, Object> privilege) {
        this.scope = Objects.requireNonNull(scope);
        if (privilege == null || privilege.isEmpty()) {
            throw new IllegalArgumentException("Privileges cannot be empty or null");
        }
        this.privilege = Collections.unmodifiableMap(privilege);
    }

    public String getScope() {
        return scope;
    }

    public Map<String, Object> getRaw() {
        return privilege;
    }

    public static GlobalScopedPrivilege fromXContent(String scope, XContentParser parser) throws IOException {
        // parser is still placed on the field name, advance to next token (field value)
        assert parser.currentToken().equals(XContentParser.Token.FIELD_NAME);
        parser.nextToken();
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
