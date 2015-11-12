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
    final Policy system;
    final PermissionCollection dynamic;

    public ESPolicy(PermissionCollection dynamic, boolean filterBadDefaults) throws Exception {
        URI policyUri = getClass().getResource(POLICY_RESOURCE).toURI();
        URI groovyUri = getClass().getResource(GROOVY_RESOURCE).toURI();
        this.template = Policy.getInstance("JavaPolicy", new URIParameter(policyUri));
        this.groovy = Policy.getInstance("JavaPolicy", new URIParameter(groovyUri));
        if (filterBadDefaults) {
            this.system = new SystemPolicy(Policy.getPolicy());
        } else {
            this.system = Policy.getPolicy();
        }
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
        return template.implies(domain, permission) || dynamic.implies(permission) || system.implies(domain, permission);
    }

    @Override
    public PermissionCollection getPermissions(CodeSource codesource) {
        // code should not rely on this method, or at least use it correctly:
        // https://bugs.openjdk.java.net/browse/JDK-8014008
        // return them a new empty permissions object so jvisualvm etc work
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if ("sun.rmi.server.LoaderHandler".equals(element.getClassName()) &&
                    "loadClass".equals(element.getMethodName())) {
                return new Permissions();
            }
        }
        // return UNSUPPORTED_EMPTY_COLLECTION since it is safe.
        return super.getPermissions(codesource);
    }

    // TODO: remove this hack when insecure defaults are removed from java

    // default policy file states:
    // "It is strongly recommended that you either remove this permission
    //  from this policy file or further restrict it to code sources
    //  that you specify, because Thread.stop() is potentially unsafe."
    // not even sure this method still works...
    static final Permission BAD_DEFAULT_NUMBER_ONE = new RuntimePermission("stopThread");

    // there are bad socket permission defaults too, but that's not locked down
    // yet it in this version.

    /**
     * Wraps the Java system policy, filtering out bad default permissions that
     * are granted to all domains.
     */
    static class SystemPolicy extends Policy {
        final Policy delegate;

        SystemPolicy(Policy delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean implies(ProtectionDomain domain, Permission permission) {
            if (BAD_DEFAULT_NUMBER_ONE.equals(permission)) {
                return false;
            }
            return delegate.implies(domain, permission);
        }
    }
}
