/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Locale;

public class FIPS140PasswordHashingAlgorithmBootstrapCheck implements BootstrapCheck {

    /**
     * Test if the node fails the check.
     *
     * @param context the bootstrap context
     * @return the result of the bootstrap check
     */
    @Override
    public BootstrapCheckResult check(final BootstrapContext context) {
        if (XPackSettings.FIPS_MODE_ENABLED.get(context.settings())) {
            final String selectedAlgorithm = XPackSettings.PASSWORD_HASHING_ALGORITHM.get(context.settings());
            if (selectedAlgorithm.toLowerCase(Locale.ROOT).startsWith("pbkdf2") == false) {
                return BootstrapCheckResult.failure("Only PBKDF2 is allowed for password hashing in a FIPS-140 JVM. Please set the " +
                        "appropriate value for [ " + XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey() + " ] setting.");
            }
        }
        return BootstrapCheckResult.success();
    }

}
