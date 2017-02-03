/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.elasticsearch.common.SuppressForbidden;

import java.net.URL;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.security.CodeSource;
import java.security.Permission;
import java.security.SecurityPermission;
import java.util.Map;

final class XPackExtensionPolicy extends Policy {
    static final Permission SET_POLICY_PERMISSION = new SecurityPermission("setPolicy");
    static final Permission GET_POLICY_PERMISSION = new SecurityPermission("getPolicy");
    static final Permission CREATE_POLICY_PERMISSION = new SecurityPermission("createPolicy.JavaPolicy");

    // the base policy (es + plugins)
    final Policy basePolicy;
    // policy extensions
    final Map<String, Policy> extensions;
    // xpack code source location
    final URL xpackURL;

    /**
     *
     * @param basePolicy The base policy
     * @param extensions Extra code source extension's policy
     */
    XPackExtensionPolicy(Policy basePolicy, Map<String, Policy> extensions) {
        this.basePolicy = basePolicy;
        this.extensions = extensions;
        xpackURL = XPackExtensionPolicy.class.getProtectionDomain().getCodeSource().getLocation();
    }

    private boolean isPolicyPermission(Permission permission) {
        return GET_POLICY_PERMISSION.equals(permission) ||
                CREATE_POLICY_PERMISSION.equals(permission) ||
                SET_POLICY_PERMISSION.equals(permission);
    }

    @Override @SuppressForbidden(reason = "fast equals check is desired")
    public boolean implies(ProtectionDomain domain, Permission permission) {
        CodeSource codeSource = domain.getCodeSource();
        if (codeSource != null && codeSource.getLocation() != null) {
            if (codeSource.getLocation().equals(xpackURL) &&
                    isPolicyPermission(permission)) {
                // forbid to get, create and set java policy in xpack codesource
                // it is only granted at startup in order to let xpack add the extensions policy
                // and make this policy the default.
                return false;
            }
            // check for an additional extension permission: extension policy is
            // only consulted for its codesources.
            Policy extension = extensions.get(codeSource.getLocation().getFile());
            if (extension != null && extension.implies(domain, permission)) {
                return true;
            }
        }
        return basePolicy.implies(domain, permission);
    }
}