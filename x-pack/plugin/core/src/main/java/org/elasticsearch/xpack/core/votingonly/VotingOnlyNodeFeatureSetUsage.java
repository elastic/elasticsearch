/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.votingonly;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;

public class VotingOnlyNodeFeatureSetUsage extends XPackFeatureSet.Usage {
    public VotingOnlyNodeFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
    }

    public VotingOnlyNodeFeatureSetUsage(boolean available) {
        super(XPackField.VOTING_ONLY, available, true);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_3_0;
    }

}
