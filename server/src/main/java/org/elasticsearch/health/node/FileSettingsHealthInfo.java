/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record FileSettingsHealthInfo(boolean isActive, long changeCount, long failureStreak, String mostRecentFailure)
    implements
        Writeable {

    /**
     * Indicates that no conclusions can be drawn about the health status.
     */
    public static final FileSettingsHealthInfo INDETERMINATE = new FileSettingsHealthInfo(false, 0L, 0, null);

    /**
     * Indicates that the health info system is active and no changes have occurred yet, so all is well.
     */
    public static final FileSettingsHealthInfo INITIAL_ACTIVE = new FileSettingsHealthInfo(true, 0L, 0, null);

    public FileSettingsHealthInfo(StreamInput in) throws IOException {
        this(in.readBoolean(), in.readVLong(), in.readVLong(), in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isActive);
        out.writeVLong(changeCount);
        out.writeVLong(failureStreak);
        out.writeOptionalString(mostRecentFailure);
    }

    public FileSettingsHealthInfo inactive() {
        return new FileSettingsHealthInfo(false, changeCount, failureStreak, mostRecentFailure);
    }

    public FileSettingsHealthInfo changed() {
        return new FileSettingsHealthInfo(isActive, changeCount + 1, failureStreak, mostRecentFailure);
    }

    public FileSettingsHealthInfo successful() {
        return new FileSettingsHealthInfo(isActive, changeCount, 0, null);
    }

    public FileSettingsHealthInfo failed(String failureDescription) {
        return new FileSettingsHealthInfo(isActive, changeCount, failureStreak + 1, failureDescription);
    }
}
