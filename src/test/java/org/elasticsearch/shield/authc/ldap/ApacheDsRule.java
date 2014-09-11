/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.junit.rules.MethodRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 *
 */
public class ApacheDsRule implements MethodRule {

    private ApacheDsEmbedded ldap;
    private final TemporaryFolder temporaryFolder;

    public ApacheDsRule(TemporaryFolder temporaryFolder) {
        this.temporaryFolder = temporaryFolder;
    }

    @Override
    public Statement apply(final Statement base, final FrameworkMethod method, final Object target) {

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    ldap = new ApacheDsEmbedded("o=sevenSeas", "seven-seas.ldif", temporaryFolder.newFolder());
                    ldap.startServer();
                    base.evaluate();
                } finally {
                    ldap.stopAndCleanup();
                }
            }
        };
    }

    public String getUrl() {
        return ldap.getUrl();
    }
}
