/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class ApacheDsRule extends ExternalResource {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private ApacheDsEmbedded ldap;
    private final TemporaryFolder temporaryFolder;

    public ApacheDsRule(TemporaryFolder temporaryFolder) {
        this.temporaryFolder = temporaryFolder;
    }

    @Override
    protected void before() throws Throwable {
        ldap = new ApacheDsEmbedded("o=sevenSeas", "seven-seas.ldif", temporaryFolder.newFolder());
        ldap.startServer();
    }

    @Override
    protected void after() {
        try {
            ldap.stopAndCleanup();
        } catch (Exception e) {
            logger.error("failed to stop and cleanup the embedded ldap server", e);
        }
    }

    public String getUrl() {
        return ldap.getUrl();
    }
}
