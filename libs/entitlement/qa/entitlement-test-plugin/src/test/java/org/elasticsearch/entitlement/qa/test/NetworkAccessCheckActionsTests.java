/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.entitlement.bridge.NotEntitledException;
import org.elasticsearch.test.ESTestCase;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;

/**
 * Regression tests for https://github.com/elastic/elasticsearch/issues/144376
 * <p>
 * In FIPS mode, {@code CertStore.getInstance("LDAP", null)} throws {@link NoSuchAlgorithmException}
 * because the LDAP CertStore provider is absent. Before the fix, this exception propagated out of
 * {@code createLDAPCertStore()}, causing the REST entitlement check handler to interpret it as an
 * entitlement denial and return HTTP 403. The "allowed" integration test then failed because it
 * expected 200.
 * <p>
 * The fix distinguishes the two sources of {@link NoSuchAlgorithmException}:
 * <ul>
 *   <li>Entitlement denial — thrown by the instrumentation as
 *       {@code new NoSuchAlgorithmException(notEntitledException)}, where the cause is a
 *       {@code NotEntitledException}</li>
 *   <li>Provider absent (FIPS) — thrown by JCA as
 *       {@code new NoSuchAlgorithmException("LDAP CertStore not available")}, so {@code getCause() == null}</li>
 * </ul>
 * Only the former is re-thrown; the latter (and any NSAE whose cause is not a {@code NotEntitledException})
 * is silently swallowed. The bridge is compile-only and loaded in a separate classloader, so the check
 * uses a class name string comparison rather than {@code instanceof}.
 */
public class NetworkAccessCheckActionsTests extends ESTestCase {

    /**
     * Verifies the normal (non-FIPS) path: when the LDAP CertStore provider is present,
     * {@code CertStore.getInstance("LDAP", null)} throws {@link java.security.InvalidAlgorithmParameterException}
     * because null params are invalid for LDAP. The first catch block handles this and the method
     * completes normally.
     */
    public void testCreateLDAPCertStore_doesNotPropagate_whenProviderPresent() throws Exception {
        assumeTrue("LDAP CertStore provider must be present for this test", Security.getProviders("CertStore.LDAP") != null);
        NetworkAccessCheckActions.createLDAPCertStore();
    }

    /**
     * Reproduces the FIPS failure: when the LDAP CertStore provider is absent,
     * {@code createLDAPCertStore()} must complete normally instead of propagating
     * {@link NoSuchAlgorithmException}.
     * <p>
     * On a standard JVM the SUN provider supplies LDAP CertStore; we simulate the FIPS
     * environment by temporarily removing it. On a FIPS JVM the provider is already absent
     * and no manipulation is needed.
     */
    public void testCreateLDAPCertStore_doesNotPropagate_whenProviderAbsent() throws Exception {
        Provider[] ldapProviders = Security.getProviders("CertStore.LDAP");

        if (ldapProviders == null || ldapProviders.length == 0) {
            // Already running in an environment without LDAP CertStore (e.g., FIPS JVM).
            // Before fix: method throws NoSuchAlgorithmException -> REST handler returns 403.
            // After fix: method completes normally -> REST handler returns 200.
            NetworkAccessCheckActions.createLDAPCertStore();
            return;
        }

        int[] positions = removeProviders(ldapProviders);
        try {
            // Before fix: CertStore.getInstance("LDAP", null) throws NoSuchAlgorithmException
            // with no cause; this propagated as-is and the REST handler mapped it to HTTP 403.
            // After fix: the exception is caught (getCause() == null => not a denial) and
            // the method returns normally, so the REST handler returns HTTP 200.
            NetworkAccessCheckActions.createLDAPCertStore();
        } finally {
            restoreProviders(ldapProviders, positions);
        }
    }

    /**
     * Verifies the entitlement-denial path: when {@code CertStore.getInstance("LDAP", null)}
     * throws {@link NoSuchAlgorithmException} whose cause is a {@link NotEntitledException} —
     * exactly as the entitlement instrumentation does — the exception must propagate out of
     * {@code createLDAPCertStore()} so the REST handler can return HTTP 403.
     * <p>
     * A temporary JCA provider overrides {@code newInstance} to throw such an exception,
     * replacing whichever real provider would otherwise handle "LDAP".
     */
    public void testCreateLDAPCertStore_rethrows_whenNSAECauseIsNotEntitledException() throws Exception {
        var simulatedNEE = new NotEntitledException("simulated denial");

        // Build a provider whose LDAP CertStore service throws NSAE(NotEntitledException), mimicking
        // the entitlement framework's denial: new NoSuchAlgorithmException(notEntitledException).
        Provider throwingProvider = new Provider("ThrowingLDAPProvider", "1.0", "test") {
            {
                putService(new Service(this, "CertStore", "LDAP", "Throwing", null, null) {
                    @Override
                    public Object newInstance(Object constructorParameter) throws NoSuchAlgorithmException {
                        throw new NoSuchAlgorithmException(simulatedNEE);
                    }
                });
            }
        };

        // Remove real LDAP CertStore providers so only ours is consulted.
        Provider[] existingLdapProviders = Security.getProviders("CertStore.LDAP");
        int[] positions = existingLdapProviders != null ? removeProviders(existingLdapProviders) : new int[0];
        Security.addProvider(throwingProvider);
        try {
            var thrown = expectThrows(NoSuchAlgorithmException.class, NetworkAccessCheckActions::createLDAPCertStore);
            assertSame(simulatedNEE, thrown.getCause());
        } finally {
            Security.removeProvider("ThrowingLDAPProvider");
            if (existingLdapProviders != null) {
                restoreProviders(existingLdapProviders, positions);
            }
        }
    }

    /**
     * Verifies that a {@link NoSuchAlgorithmException} with a non-null cause that is NOT a
     * {@link NotEntitledException} is silently swallowed. This guards against incorrectly
     * re-throwing any NSAE that happens to have a cause (e.g. a wrapped provider error).
     */
    public void testCreateLDAPCertStore_doesNotRethrow_whenNSAECauseIsNotNEE() throws Exception {
        var unrelatedCause = new RuntimeException("some other provider failure");

        Provider throwingProvider = new Provider("ThrowingLDAPProvider", "1.0", "test") {
            {
                putService(new Service(this, "CertStore", "LDAP", "Throwing", null, null) {
                    @Override
                    public Object newInstance(Object constructorParameter) throws NoSuchAlgorithmException {
                        throw new NoSuchAlgorithmException(unrelatedCause);
                    }
                });
            }
        };

        Provider[] existingLdapProviders = Security.getProviders("CertStore.LDAP");
        int[] positions = existingLdapProviders != null ? removeProviders(existingLdapProviders) : new int[0];
        Security.addProvider(throwingProvider);
        try {
            // Must complete normally — a non-NEE cause is not an entitlement denial.
            NetworkAccessCheckActions.createLDAPCertStore();
        } finally {
            Security.removeProvider("ThrowingLDAPProvider");
            if (existingLdapProviders != null) {
                restoreProviders(existingLdapProviders, positions);
            }
        }
    }

    /**
     * Removes all given providers and returns their original 1-based positions for
     * restoration via {@link #restoreProviders}. Providers are removed in the order
     * supplied; positions are recorded before any removal takes place.
     */
    private static int[] removeProviders(Provider[] providers) {
        Provider[] allProviders = Security.getProviders();
        int[] positions = new int[providers.length];
        for (int j = 0; j < providers.length; j++) {
            int found = -1;
            for (int i = 0; i < allProviders.length; i++) {
                if (allProviders[i] == providers[j]) {
                    found = i + 1; // insertProviderAt uses 1-based index
                    break;
                }
            }
            assertNotEquals("Provider " + providers[j].getName() + " not found in installed providers", -1, found);
            positions[j] = found;
            Security.removeProvider(providers[j].getName());
        }
        return positions;
    }

    /**
     * Restores providers to their original positions after a call to {@link #removeProviders}.
     * Providers must be re-inserted in forward order so that each insertion shifts only the
     * elements that were originally after it, leaving earlier slots undisturbed.
     */
    private static void restoreProviders(Provider[] providers, int[] positions) {
        for (int j = 0; j < providers.length; j++) {
            Security.insertProviderAt(providers[j], positions[j]);
        }
    }
}
