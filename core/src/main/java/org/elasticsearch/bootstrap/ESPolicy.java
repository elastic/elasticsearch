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

import java.net.SocketPermission;
import java.net.URL;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.Map;

/** custom policy for union of static and dynamic permissions */
final class ESPolicy extends Policy {
    
    /** template policy file, the one used in tests */
    static final String POLICY_RESOURCE = "security.policy";
    /** limited policy for scripts */
    static final String UNTRUSTED_RESOURCE = "untrusted.policy";
    
    final Policy template;
    final Policy untrusted;
    final Policy system;
    final PermissionCollection dynamic;
    final Map<String,Policy> plugins;

    public ESPolicy(PermissionCollection dynamic, Map<String,Policy> plugins, boolean filterBadDefaults) {
        this.template = Security.readPolicy(getClass().getResource(POLICY_RESOURCE), JarHell.parseClassPath());
        this.untrusted = Security.readPolicy(getClass().getResource(UNTRUSTED_RESOURCE), new URL[0]);
        if (filterBadDefaults) {
            this.system = new SystemPolicy(Policy.getPolicy());
        } else {
            this.system = Policy.getPolicy();
        }
        this.dynamic = dynamic;
        this.plugins = plugins;
    }

    @Override @SuppressForbidden(reason = "fast equals check is desired")
    public boolean implies(ProtectionDomain domain, Permission permission) {        
        CodeSource codeSource = domain.getCodeSource();
        // codesource can be null when reducing privileges via doPrivileged()
        if (codeSource == null) {
            return false;
        }

        URL location = codeSource.getLocation();
        // location can be null... ??? nobody knows
        // https://bugs.openjdk.java.net/browse/JDK-8129972
        if (location != null) {
            // run scripts with limited permissions
            if (BootstrapInfo.UNTRUSTED_CODEBASE.equals(location.getFile())) {
                return untrusted.implies(domain, permission);
            }
            // check for an additional plugin permission: plugin policy is
            // only consulted for its codesources.
            Policy plugin = plugins.get(location.getFile());
            if (plugin != null && plugin.implies(domain, permission)) {
                return true;
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

    // default policy file states:
    // "allows anyone to listen on dynamic ports"
    // specified exactly because that is what we want, and fastest since it won't imply any
    // expensive checks for the implicit "resolve"
    static final Permission BAD_DEFAULT_NUMBER_TWO = new SocketPermission("localhost:0", "listen");

    /**
     * Wraps the Java system policy, filtering out bad default permissions that
     * are granted to all domains. Note, before java 8 these were even worse.
     */
    static class SystemPolicy extends Policy {
        final Policy delegate;

        SystemPolicy(Policy delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean implies(ProtectionDomain domain, Permission permission) {
            if (BAD_DEFAULT_NUMBER_ONE.equals(permission) || BAD_DEFAULT_NUMBER_TWO.equals(permission)) {
                return false;
            }
            return delegate.implies(domain, permission);
        }
    }
}
