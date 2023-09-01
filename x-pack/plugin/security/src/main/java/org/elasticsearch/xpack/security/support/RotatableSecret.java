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
import java.util.concurrent.locks.StampedLock;

/**
 * Helper class to provide a secret that can be rotated. Once rotated the prior secret is available for a configured amount of time before
 * it is invalidated. This allows for secrete rotation without temporary failures or the need to tightly orchestrate multiple parties.
 * This class is threadsafe, however it is also assumes that matching secrets are frequent but rotation is a rare.
 */
public class RotatableSecret {
    private Secrets secrets;
    private final StampedLock stampedLock = new StampedLock();

    /**
     * @param secret The secret to rotate. {@code null} if the secret is not configured.
     */
    public RotatableSecret(@Nullable SecureString secret) {
        this.secrets = new Secrets(Strings.hasText(secret) ? secret : null, null, Instant.EPOCH);
    }

    /**
     * Rotates the secret iff the new secret and current secret are different. If rotated, the current secret is moved to the prior secret
     * which is valid for the given grace period and new secret is now considered the current secret.
     * @param newSecret the secret to rotate in.
     * @param gracePeriod the time period that the prior secret is valid.
     */
    public void rotate(SecureString newSecret, TimeValue gracePeriod) {
        long stamp = stampedLock.writeLock();
        try {
            if (this.secrets.current.equals(newSecret) == false) {
                secrets = new Secrets(
                    Strings.hasText(newSecret) ? newSecret : null,
                    secrets.current,
                    Instant.now().plusMillis(gracePeriod.getMillis())
                );
            }
        } finally {
            stampedLock.unlockWrite(stamp);
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
     * @return true if either the current or (non-expired) prior secret matches.
     * false if nether match. false if current and prior secret are unset. false if passed in secret is null or empty
     */
    public boolean matches(SecureString secret) {
        checkExpired();
        if ((Strings.hasText(secrets.current) == false && Strings.hasText(secrets.prior) == false) || Strings.hasText(secret) == false) {
            return false;
        }
        return secrets.current.equals(secret) || (secrets.prior != null && secrets.prior.equals(secret));
    }

    // for testing purpose only
    public Secrets getSecrets() {
        return secrets;
    }

    private void checkExpired() {
        boolean needToUnlock = false;
        long stamp = stampedLock.tryOptimisticRead();
        boolean expired = secrets.prior != null && secrets.priorValidTill.isBefore(Instant.now()); // optimistic read
        if (stampedLock.validate(stamp) == false) {
            // optimism failed...potentially block to obtain the read lock and try the read again
            stamp = stampedLock.readLock();
            needToUnlock = true;
            expired = secrets.prior != null && secrets.priorValidTill.isBefore(Instant.now()); // locked read
        }
        try {
            if (expired) {
                stamp = stampedLock.tryConvertToWriteLock(stamp);// upgrade the read lock
                if (stamp == 0) {
                    // block until we can acquire the write lock
                    stamp = stampedLock.writeLock();
                }
                needToUnlock = true;
                SecureString prior = secrets.prior;
                secrets = new Secrets(secrets.current, null, Instant.EPOCH);
                prior.close(); // zero out the memory
            }
        } finally {
            if (needToUnlock) { // only unlock if we acquired a read or write lock
                stampedLock.unlock(stamp);
            }
        }
    }

    public record Secrets(SecureString current, SecureString prior, Instant priorValidTill) {};
}
