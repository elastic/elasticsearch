/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.xpack.core.XPackSettings;

import javax.crypto.SecretKeyFactory;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

/**
 * Bootstrap check to ensure that one of the allowed password hashing algorithms is
 * selected and that it is available.
 */
public class PasswordHashingAlgorithmBootstrapCheck implements BootstrapCheck {
    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        final String selectedAlgorithm = XPackSettings.PASSWORD_HASHING_ALGORITHM.get(context.settings);
        if (selectedAlgorithm.toLowerCase(Locale.ROOT).startsWith("pbkdf2")) {
            try {
                SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
            } catch (NoSuchAlgorithmException e) {
                final String errorMessage = String.format(Locale.ROOT,
                    "Support for PBKDF2WithHMACSHA512 must be available in order to use any of the " +
                        "PBKDF2 algorithms for the [%s] setting.", XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey());
                return BootstrapCheckResult.failure(errorMessage);
            }
        }
        return BootstrapCheckResult.success();
    }

    @Override
    public boolean alwaysEnforce() {
        return true;
    }
}
