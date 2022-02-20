/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.ReflectPermission;
import java.net.SocketPermission;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.Permission;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

import javax.security.auth.AuthPermission;
import javax.security.auth.PrivateCredentialPermission;
import javax.security.auth.kerberos.ServicePermission;

/**
 * Oversees all the security specific logic for the HDFS Repository plugin.
 *
 * Keeps track of the current user for a given repository, as well as which
 * permissions to grant the blob store restricted execution methods.
 */
class HdfsSecurityContext {

    private static final Permission[] SIMPLE_AUTH_PERMISSIONS;
    private static final Permission[] KERBEROS_AUTH_PERMISSIONS;
    static {
        // We can do FS ops with only a few elevated permissions:
        SIMPLE_AUTH_PERMISSIONS = new Permission[] {
            new SocketPermission("*", "connect"),
            // 1) hadoop dynamic proxy is messy with access rules
            new ReflectPermission("suppressAccessChecks"),
            // 2) allow hadoop to add credentials to our Subject
            new AuthPermission("modifyPrivateCredentials"),
            // 3) RPC Engine requires this for re-establishing pooled connections over the lifetime of the client
            new PrivateCredentialPermission("org.apache.hadoop.security.Credentials * \"*\"", "read") };

        // If Security is enabled, we need all the following elevated permissions:
        KERBEROS_AUTH_PERMISSIONS = new Permission[] {
            new SocketPermission("*", "connect"),
            // 1) hadoop dynamic proxy is messy with access rules
            new ReflectPermission("suppressAccessChecks"),
            // 2) allow hadoop to add credentials to our Subject
            new AuthPermission("modifyPrivateCredentials"),
            // 3) allow hadoop to act as the logged in Subject
            new AuthPermission("doAs"),
            // 4) Listen and resolve permissions for kerberos server principals
            new SocketPermission("localhost:0", "listen,resolve"),
            // We add the following since hadoop requires the client to re-login when the kerberos ticket expires:
            // 5) All the permissions needed for UGI to do its weird JAAS hack
            new RuntimePermission("getClassLoader"),
            new RuntimePermission("setContextClassLoader"),
            // 6) Additional permissions for the login modules
            new AuthPermission("modifyPrincipals"),
            new PrivateCredentialPermission("org.apache.hadoop.security.Credentials * \"*\"", "read"),
            new PrivateCredentialPermission("javax.security.auth.kerberos.KerberosTicket * \"*\"", "read"),
            new PrivateCredentialPermission("javax.security.auth.kerberos.KeyTab * \"*\"", "read")
            // Included later:
            // 7) allow code to initiate kerberos connections as the logged in user
            // Still far and away fewer permissions than the original full plugin policy
        };
    }

    /**
     * Locates the keytab file in the environment and verifies that it exists.
     * Expects keytab file to exist at {@code $CONFIG_DIR$/repository-hdfs/krb5.keytab}
     */
    static Path locateKeytabFile(Environment environment) {
        Path keytabPath = environment.configFile().resolve("repository-hdfs").resolve("krb5.keytab");
        try {
            if (Files.exists(keytabPath) == false) {
                throw new RuntimeException("Could not locate keytab at [" + keytabPath + "].");
            }
        } catch (SecurityException se) {
            throw new RuntimeException("Could not locate keytab at [" + keytabPath + "]", se);
        }
        return keytabPath;
    }

    private final UserGroupInformation ugi;
    private final boolean restrictPermissions;
    private final Permission[] restrictedExecutionPermissions;

    HdfsSecurityContext(UserGroupInformation ugi, boolean restrictPermissions) {
        this.ugi = ugi;
        this.restrictPermissions = restrictPermissions;
        this.restrictedExecutionPermissions = renderPermissions(ugi);
    }

    private Permission[] renderPermissions(UserGroupInformation userGroupInformation) {
        Permission[] permissions;
        if (userGroupInformation.isFromKeytab()) {
            // KERBEROS
            // Leave room to append one extra permission based on the logged in user's info.
            int permlen = KERBEROS_AUTH_PERMISSIONS.length + 1;
            permissions = new Permission[permlen];

            System.arraycopy(KERBEROS_AUTH_PERMISSIONS, 0, permissions, 0, KERBEROS_AUTH_PERMISSIONS.length);

            // Append a kerberos.ServicePermission to only allow initiating kerberos connections
            // as the logged in user.
            permissions[permissions.length - 1] = new ServicePermission(userGroupInformation.getUserName(), "initiate");
        } else {
            // SIMPLE
            permissions = Arrays.copyOf(SIMPLE_AUTH_PERMISSIONS, SIMPLE_AUTH_PERMISSIONS.length);
        }
        return permissions;
    }

    private Permission[] getRestrictedExecutionPermissions() {
        return restrictedExecutionPermissions;
    }

    <T> T doPrivilegedOrThrow(PrivilegedExceptionAction<T> action) throws IOException {
        SpecialPermission.check();
        try {
            if (restrictPermissions) {
                return AccessController.doPrivileged(action, null, this.getRestrictedExecutionPermissions());
            } else {
                return AccessController.doPrivileged(action);
            }
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

    void ensureLogin() {
        if (ugi.isFromKeytab()) {
            try {
                ugi.checkTGTAndReloginFromKeytab();
            } catch (IOException ioe) {
                throw new UncheckedIOException("Could not re-authenticate", ioe);
            }
        }
    }
}
