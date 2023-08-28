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
 * This class is mostly threadsafe-ish (nothing will blowup), however it is also not designed multiple concurrent
 * rotations of the same secret as the functions are non-blocking and non-atomic. It is expected that the caller will not attempt to
 * rotate different secrets concurrently. TODO: think about this some more
 */
public class RotatableSecret {
    private volatile Secrets secrets;

    /**
     * @param secret The secret to rotate. {@code null} if the secret is not configured.
     */
    public RotatableSecret(@Nullable SecureString secret) {
        this.secrets = new Secrets(Strings.hasText(secret) ? secret : null, null, Instant.EPOCH);
    }

    public void rotate(SecureString newSecret, TimeValue gracePeriod) {
        Secrets newSecretRecord = new Secrets(
            Strings.hasText(newSecret) ? newSecret : null,
            secrets.current,
            Instant.now().plusMillis(gracePeriod.getMillis())
        );
        if(this.secrets.equals(newSecretRecord) == false) {
            secrets = newSecretRecord;
        }
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
        if (isSet() == false || Strings.hasText(secret) == false) { // calls checkExpired
            return false;
        }
        assert secrets.current != null;
        return secrets.current.equals(secret) || (secrets.prior != null && secrets.prior.equals(secret));
    }

    //for testing purpose
    public Secrets getSecrets() {
        return secrets;
    }

    private void checkExpired() {
        if (secrets.prior != null && secrets.validTill.isBefore(Instant.now())) {
            // nuke the prior secret
            Secrets newSecretsRecord = new Secrets(secrets.current, null, Instant.EPOCH);
            secrets.prior.close();
            secrets = newSecretsRecord;
        }
    }

    public record Secrets(SecureString current, SecureString prior, Instant validTill) {};
}
