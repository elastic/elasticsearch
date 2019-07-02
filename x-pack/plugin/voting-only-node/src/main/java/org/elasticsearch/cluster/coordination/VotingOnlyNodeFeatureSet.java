/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.votingonly.VotingOnlyNodeFeatureSetUsage;

import java.util.Map;

public class VotingOnlyNodeFeatureSet implements XPackFeatureSet {

    private final XPackLicenseState licenseState;

    @Inject
    public VotingOnlyNodeFeatureSet(@Nullable XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.VOTING_ONLY;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isVotingOnlyAllowed();
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        listener.onResponse(new VotingOnlyNodeFeatureSetUsage(licenseState.isVotingOnlyAllowed()));
    }
}
