/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.settings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.time.Instant;
import java.util.concurrent.locks.StampedLock;

/**
 * A container for a {@link SecureString} that can be rotated with a grace period for the secret that has been rotated out.
 * Once rotated the prior secret is available for a configured amount of time before it is invalidated.
 * This allows for secret rotation without temporary failures or the need to tightly orchestrate
 * multiple parties. This class is threadsafe, however it is also assumes that reading secrets are frequent (i.e. every request)
 * but rotation is a rare (i.e. once a day).
 */
public class RotatableSecret {
    private Secrets secrets;
    private final StampedLock stampedLock = new StampedLock();

    /**
     * @param secret The secret to rotate. {@code null} if the secret is not configured.
     */
    public RotatableSecret(@Nullable SecureString secret) {
        this.secrets = new Secrets(Strings.hasText(secret) ? secret.clone() : null, null, Instant.EPOCH);
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
            if (secrets.current == null || secrets.current.equals(newSecret) == false) {
                secrets = new Secrets(
                    Strings.hasText(newSecret) ? newSecret.clone() : null,
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
        if (Strings.hasText(secret) == false) {
            return false;
        }
        boolean currentSecretValid = Strings.hasText(secrets.current);
        boolean priorSecretValid = Strings.hasText(secrets.prior);
        if (currentSecretValid && secrets.current.equals(secret)) {
            return true;
        } else {
            return priorSecretValid && secrets.prior.equals(secret);
        }
    }

    // for testing only
    Secrets getSecrets() {
        return secrets;
    }

    // for testing only
    boolean isWriteLocked() {
        return stampedLock.isWriteLocked();
    }

    /**
     * Checks to see if the prior secret TTL has expired. If expired, evict from the backing data structure. Always call this before
     * reading the secret(s).
     */
    private void checkExpired() {
        boolean needToUnlock = false;
        long stamp = stampedLock.tryOptimisticRead();
        boolean expired = secrets.prior != null && secrets.priorValidTill.compareTo(Instant.now()) <= 0; // optimistic read
        if (stampedLock.validate(stamp) == false) {
            // optimism failed...potentially block to obtain the read lock and try the read again
            stamp = stampedLock.readLock();
            needToUnlock = true;
            expired = secrets.prior != null && secrets.priorValidTill.compareTo(Instant.now()) <= 0; // locked read
        }
        try {
            if (expired) {
                long stampUpgrade = stampedLock.tryConvertToWriteLock(stamp);
                if (stampUpgrade == 0) {
                    // upgrade failed so we need to manually unlock the read lock and grab the write lock
                    if (needToUnlock) {
                        stampedLock.unlockRead(stamp);
                    }
                    stamp = stampedLock.writeLock();
                    expired = secrets.prior != null && secrets.priorValidTill.isBefore(Instant.now()); // check again since we had to unlock
                } else {
                    stamp = stampUpgrade;
                }
                needToUnlock = true;
                if (expired) {
                    SecureString prior = secrets.prior;
                    secrets = new Secrets(secrets.current, null, Instant.EPOCH);
                    prior.close(); // zero out the memory
                }
            }
        } finally {
            if (needToUnlock) { // only unlock if we acquired a read or write lock
                stampedLock.unlock(stamp);
            }
        }
    }

    public record Secrets(SecureString current, SecureString prior, Instant priorValidTill) {};
}
