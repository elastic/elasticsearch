/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.TransportVersions.ML_TRANSFORM_SYSTEM_INDEX_MIGRATE_REASON;

public class SetUpgradeModeActionResponse extends AcknowledgedResponse {
    private final boolean alreadyInUpgradeMode;

    public SetUpgradeModeActionResponse(StreamInput in) throws IOException {
        super(in);
        this.alreadyInUpgradeMode = in.getTransportVersion().onOrAfter(ML_TRANSFORM_SYSTEM_INDEX_MIGRATE_REASON) && in.readBoolean();
    }

    public SetUpgradeModeActionResponse(boolean acknowledged, boolean alreadyInUpgradeMode) {
        super(acknowledged);
        this.alreadyInUpgradeMode = alreadyInUpgradeMode;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(ML_TRANSFORM_SYSTEM_INDEX_MIGRATE_REASON)) {
            out.writeBoolean(alreadyInUpgradeMode);
        }
    }

    public boolean alreadyInUpgradeMode() {
        return alreadyInUpgradeMode;
    }
}
