/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.votingonly;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;

public class VotingOnlyNodeFeatureSetUsage extends XPackFeatureSet.Usage {
    public VotingOnlyNodeFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
    }

    public VotingOnlyNodeFeatureSetUsage() {
        super(XPackField.VOTING_ONLY, true, true);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_3_0;
    }

}
