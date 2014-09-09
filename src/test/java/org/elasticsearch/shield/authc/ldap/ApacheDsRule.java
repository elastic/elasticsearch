/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 *
 */
public class ApacheDsRule implements MethodRule {

    private ApacheDsEmbedded ldap;

    @Override
    public Statement apply(final Statement base, final FrameworkMethod method, final Object target) {

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    ldap = new ApacheDsEmbedded("o=sevenSeas", "seven-seas.ldif", target.getClass().getName());
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
