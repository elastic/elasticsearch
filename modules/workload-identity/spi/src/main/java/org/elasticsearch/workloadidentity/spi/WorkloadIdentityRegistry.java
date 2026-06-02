/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity.spi;

/**
 * Static slot for the node-wide {@link WorkloadIdentityIssuerClient}, set by the workload-identity
 * plugin's {@code createComponents} and read by extending plugins on non-DI code paths via
 * {@link #getSharedIssuerClient}.
 *
 * <p>Lives in the SPI jar so the workload-identity module and every extender resolve it through
 * the same {@code spiClassLoader} — one {@code Class} object, one static slot.
 *
 * <p>DI-built consumers should prefer the {@link WorkloadIdentityIssuerClient} binding published
 * as a {@code PluginComponentBinding} from the same {@code createComponents} and skip the static
 * lookup.
 */
public final class WorkloadIdentityRegistry {

    // volatile is sufficient: the producer writes once from createComponents, which happens-before
    // every consumer read (via the DI graph or this accessor); reset() re-establishes that edge
    // for tests that construct multiple plugin instances.
    private static volatile WorkloadIdentityIssuerClient issuerClient;

    private WorkloadIdentityRegistry() {}

    /**
     * @return the node-wide {@link WorkloadIdentityIssuerClient}. Always non-null after the
     *         workload-identity plugin's {@code createComponents} has run; callers should consult
     *         {@link WorkloadIdentityIssuerClient#isEnabled()} to know whether token issuance is
     *         actually available on this node.
     * @throws IllegalStateException if invoked before the workload-identity plugin has wired the client.
     */
    public static WorkloadIdentityIssuerClient getSharedIssuerClient() {
        WorkloadIdentityIssuerClient client = issuerClient;
        if (client == null) {
            throw new IllegalStateException("WorkloadIdentityIssuerClient is not constructed yet");
        }
        return client;
    }

    /**
     * Publish the node-wide issuer client. Invoked by the workload-identity plugin from
     * {@code createComponents} after the active client (or its {@code Inactive*} stub) has been
     * constructed. Subsequent invocations without an intervening {@link #reset()} overwrite the
     * slot; production code does not exercise that path, but the test infrastructure does (see
     * {@code MockPluginsService}, which constructs multiple plugin instances within a single JVM).
     */
    public static void setIssuerClient(WorkloadIdentityIssuerClient client) {
        issuerClient = client;
    }

    /**
     * Clear the slot so a subsequent {@link #setIssuerClient} call publishes a new client. The
     * workload-identity plugin's constructor calls this so that constructing a second plugin
     * instance (e.g. a fresh node in a JVM that already constructed one, as in sequential test
     * setups and full-cluster-restart helpers) does not see the previous instance's client.
     */
    public static void reset() {
        issuerClient = null;
    }
}
