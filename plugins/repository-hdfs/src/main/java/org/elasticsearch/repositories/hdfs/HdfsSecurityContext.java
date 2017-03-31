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
package org.elasticsearch.repositories.hdfs;

import java.io.FilePermission;
import java.io.IOException;
import java.lang.reflect.ReflectPermission;
import java.net.SocketPermission;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
import java.util.Arrays;
import javax.security.auth.AuthPermission;
import javax.security.auth.PrivateCredentialPermission;
import javax.security.auth.kerberos.ServicePermission;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;

/**
 * Oversees all the security specific logic for the HDFS Repository plugin.
 *
 * Keeps track of the current user for a given repository, as well as which
 * permissions to grant the blob store restricted execution methods.
 */
public class HdfsSecurityContext {

    private static final Logger LOGGER = Loggers.getLogger(HdfsSecurityContext.class);

    private static final Permission[] SIMPLE_AUTH_PERMISSIONS;
    private static final Permission[] KERBEROS_AUTH_PERMISSIONS;
    static {
        // We can do FS ops with only a few elevated permissions:
        SIMPLE_AUTH_PERMISSIONS = new Permission[]{
            new SocketPermission("*", "connect"),
            // 1) hadoop dynamic proxy is messy with access rules
            new ReflectPermission("suppressAccessChecks"),
            // 2) allow hadoop to add credentials to our Subject
            new AuthPermission("modifyPrivateCredentials")
        };

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
            // 5) All the permissions needed for UGI to do it's weird JAAS hack
            new RuntimePermission("getClassLoader"),
            new RuntimePermission("setContextClassLoader"),
            // 6) Additional permissions for the login modules
            new AuthPermission("modifyPrincipals"),
            new PrivateCredentialPermission("org.apache.hadoop.security.Credentials * \"*\"", "read"),
            new PrivateCredentialPermission("javax.security.auth.kerberos.KerberosTicket * \"*\"", "read"),
            new PrivateCredentialPermission("javax.security.auth.kerberos.KeyTab * \"*\"", "read")
            // Included later:
            // 7) allow code to initiate kerberos connections as the logged in user
            // 8) allow code to access kerberos keytab file on disk
            // Still far and away fewer permissions than the original full plugin policy
        };
    }

    private static final String PROP_KRB5_KEYTAB = "krb5.keytab";

    private static String getKeytabFileName() {
        String keytabLocation = System.getProperty(PROP_KRB5_KEYTAB);
        if (keytabLocation == null) {
            LOGGER.error("Keytab file location is not set in the [{}] system property.", PROP_KRB5_KEYTAB);
            throw new RuntimeException("Could not locate keytab");
        }
        return keytabLocation;
    }

    /**
     * Locates the keytab file from the system properties and verifies that it exists.
     */
    @SuppressForbidden(reason = "PathUtils.get() for finding configured keytab file location")
    public static String locateKeytabFile() {
        String keytabLocation = getKeytabFileName();
        try {
            Path keytabPath = PathUtils.get(keytabLocation);
            if (Files.exists(keytabPath) == false) {
                LOGGER.error("Could not locate keytab file at [{}]. Check that the [{}] system property is correct.",
                    keytabLocation, PROP_KRB5_KEYTAB);
                throw new RuntimeException("Could not locate keytab");
            }
        } catch (SecurityException se) {
            LOGGER.error("Denied access to keytab file at [{}]. Check that the [{}] system property is correct.",
                keytabLocation, PROP_KRB5_KEYTAB);
            LOGGER.error("Could not access keytab", se);
            throw new RuntimeException("Could not locate keytab");
        }
        return keytabLocation;
    }

    private final UserGroupInformation ugi;
    private final Permission[] restrictedExecutionPermissions;

    public HdfsSecurityContext(UserGroupInformation ugi) {
        this.ugi = ugi;
        this.restrictedExecutionPermissions = renderPermissions(ugi);
    }

    private Permission[] renderPermissions(UserGroupInformation ugi) {
        Permission[] permissions;
        if (ugi.isFromKeytab()) {
            // KERBEROS
            // Leave room to append two permissions based on the logged in user's info.
            int permlen = KERBEROS_AUTH_PERMISSIONS.length + 2;
            permissions = new Permission[permlen];

            System.arraycopy(KERBEROS_AUTH_PERMISSIONS, 0, permissions, 0, KERBEROS_AUTH_PERMISSIONS.length);

            // Append a kerberos.ServicePermission to only allow initiating kerberos connections
            // as the logged in user.
            permissions[permissions.length-2] = new ServicePermission(ugi.getUserName(), "initiate");
            // Append a file permission for reading the keytab file.
            permissions[permissions.length-1] = new FilePermission(getKeytabFileName(), "read");
        } else {
            // SIMPLE
            permissions = Arrays.copyOf(SIMPLE_AUTH_PERMISSIONS, SIMPLE_AUTH_PERMISSIONS.length);
        }
        return permissions;
    }

    public UserGroupInformation getUser() {
        return ugi;
    }

    public Permission[] getRestrictedExecutionPermissions() {
        return restrictedExecutionPermissions;
    }

    public void ensureLogin() {
        if (ugi.isFromKeytab()) {
            try {
                ugi.checkTGTAndReloginFromKeytab();
            } catch (IOException ioe) {
                throw new RuntimeException("Could not re-authenticate", ioe);
            }
        }
    }
}
