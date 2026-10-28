/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Oversees all the security specific logic for the HDFS Repository plugin.
 *
 * Keeps track of the current user for a given repository.
 */
class HdfsSecurityContext {

    /**
     * Locates the keytab file in the environment and verifies that it exists.
     * Expects keytab file to exist at {@code $CONFIG_DIR$/repository-hdfs/krb5.keytab}
     */
    static Path locateKeytabFile(Environment environment) {
        Path keytabPath = environment.configDir().resolve("repository-hdfs").resolve("krb5.keytab");
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

    HdfsSecurityContext(UserGroupInformation ugi) {
        this.ugi = ugi;
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
