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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.SuppressForbidden;

import java.net.URI;
import java.net.URL;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.security.URIParameter;
import java.util.PropertyPermission;

/** custom policy for union of static and dynamic permissions */
final class ESPolicy extends Policy {
    
    /** template policy file, the one used in tests */
    static final String POLICY_RESOURCE = "security.policy";
    /** limited policy for groovy scripts */
    static final String GROOVY_RESOURCE = "groovy.policy";
    
    final Policy template;
    final Policy groovy;
    final PermissionCollection dynamic;

    public ESPolicy(PermissionCollection dynamic) throws Exception {
        URI policyUri = getClass().getResource(POLICY_RESOURCE).toURI();
        URI groovyUri = getClass().getResource(GROOVY_RESOURCE).toURI();
        this.template = Policy.getInstance("JavaPolicy", new URIParameter(policyUri));
        this.groovy = Policy.getInstance("JavaPolicy", new URIParameter(groovyUri));
        this.dynamic = dynamic;
    }

    @Override @SuppressForbidden(reason = "fast equals check is desired")
    public boolean implies(ProtectionDomain domain, Permission permission) {        
        CodeSource codeSource = domain.getCodeSource();
        // codesource can be null when reducing privileges via doPrivileged()
        if (codeSource != null) {
            URL location = codeSource.getLocation();
            // location can be null... ??? nobody knows
            // https://bugs.openjdk.java.net/browse/JDK-8129972
            if (location != null) {
                // run groovy scripts with no permissions (except logging property)
                if ("/groovy/script".equals(location.getFile())) {
                    return groovy.implies(domain, permission);
                }
            }
        }

        // otherwise defer to template + dynamic file permissions
        return template.implies(domain, permission) || dynamic.implies(permission);
    }
}
