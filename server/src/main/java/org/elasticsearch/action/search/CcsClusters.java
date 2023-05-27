/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.Strings;

import java.util.Objects;

/**
 * Extension of the Clusters class for CCS uses. It adds the number of remote clusters
 * included in a search and whether ccs_minimize_roundtrips is true.
 *
 * Like Clusters, this is an immutable class.
 *
 * Unlike Clusters, (for now at least) the new fields are not Writeable. They are only
 * used during CCS searches for accounting on the primary coordinator.
 */
public class CcsClusters extends Clusters {

    // For now we use the Writeable implementation of the parent class and mark the new fields as transient
    // We may revisit this later if we need to save this info to the .async-search system index.
    private final transient int remoteClusters;
    private final transient boolean ccsMinimizeRoundtrips;

    public CcsClusters(int total, int successful, int skipped, int remoteClusters, boolean ccsMinimizeRoundtrips) {
        super(total, successful, skipped, false);
        assert remoteClusters <= total : "total: " + total + " remote: " + remoteClusters;
        assert ccsMinimizeRoundtrips == false || remoteClusters > 0
            : "ccsMinimizeRoundtrips is true but remoteClusters count is not a positive number: " + remoteClusters;
        int localCount = total - remoteClusters;
        assert localCount == 0 || localCount == 1 : "total - remoteClusters should only be 0 or 1";

        this.remoteClusters = remoteClusters;
        this.ccsMinimizeRoundtrips = ccsMinimizeRoundtrips;
    }

    /**
     * @return how many remote clusters were using during the execution of the search request
     * If not set, returns -1, meaning 'unknown'.
     */
    public int getRemoteClusters() {
        return remoteClusters;
    }

    /**
     * @return whether this search was a cross cluster search done with ccsMinimizeRoundtrips=true
     */
    public boolean isCcsMinimizeRoundtrips() {
        return ccsMinimizeRoundtrips;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) {
            return false;
        }
        Clusters clusters = (Clusters) o;
        return remoteClusters == clusters.getRemoteClusters() && ccsMinimizeRoundtrips == clusters.isCcsMinimizeRoundtrips();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTotal(), getSuccessful(), getSkipped(), getRemoteClusters(), isCcsMinimizeRoundtrips());
    }

    @Override
    public String toString() {
        return Strings.format(
            "AsyncCcsClusters{total=%d, successful=%d, skipped=%d, remote=%d, ccsMinimizeRoundtrips=%s}",
            getTotal(),
            getSuccessful(),
            getSkipped(),
            remoteClusters,
            ccsMinimizeRoundtrips
        );
    }
}
