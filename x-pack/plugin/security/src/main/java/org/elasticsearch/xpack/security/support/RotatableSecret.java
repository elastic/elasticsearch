/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.time.Instant;

/**
 * Helper class to provide a secret that can be rotated. Once rotated the prior secret is available for a configured amount of time before
 * it is invalidated. This allows for secrete rotation without temporary failures or the need to tightly orchestrate multiple parties.
 */
public class RotatableSecret {
    private volatile Secrets secrets;

    /**
     * @param secret The secret to rotate. {@code null} if the secret is not configured.
     */
    public RotatableSecret(@Nullable SecureString secret) {
        this.secrets = new Secrets(secret, null, Instant.EPOCH);
    }

    public void rotate(SecureString newSecret, TimeValue priorValidFor) {
        // set the current secret and move the current to the prior
        secrets = new Secrets(newSecret, secrets.current, Instant.now().plusMillis(priorValidFor.getMillis()));
    }

    /**
     * @return true if the current or prior value has a non-null and a non-empty value
     */
    public boolean isSet() {
        checkExpired();
        return Strings.hasText(secrets.current) || Strings.hasText(secrets.prior);
    }

    /**
     * Check to see if the current or (non-expired) prior secret matches the passed in secret.
     * @param secret The secret to match against.
     * @return true if either the current or (non-expired) prior secret matches. false if nether match. false if nether are currently set.
     */
    public boolean matches(SecureString secret) {
        if (isSet() == false) { //calls checkExpired
            return false;
        }
        return secrets.current.equals(secret) || secrets.prior.equals(secret);
    }

    private void checkExpired() {
        if (secrets.prior != null && secrets.validTill.isBefore(Instant.now())) {
            // nuke the prior secret
           secrets = new Secrets(secrets.current, null, Instant.EPOCH);
        }
    }

    record Secrets(SecureString current, SecureString prior, Instant validTill) {};
}
