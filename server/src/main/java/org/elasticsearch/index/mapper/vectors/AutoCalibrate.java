/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibrationProfile;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

/** Parsed {@code auto_calibrate} index option: the resolved profile plus the value the user originally supplied. */
public record AutoCalibrate(@Nullable Object originalValue, IvfAutoCalibrationProfile profile) implements ToXContentFragment {
    static final String NAME = "auto_calibrate";
    static final AutoCalibrate DEFAULT = new AutoCalibrate(null, IvfAutoCalibrationProfile.DISABLED);

    public static IvfAutoCalibrationProfile defaultEnabledProfile(IndexVersion indexVersion) {
        return IvfAutoCalibrationProfile.QUALITY;
    }

    static AutoCalibrate defaultAutoCalibrate(IndexVersion indexVersion) {
        return DEFAULT;
    }

    /** Accepts a boolean, a boolean string, or a profile name. */
    static AutoCalibrate parse(@Nullable Object node, IndexVersion indexVersion, String fieldName) {
        if (node == null) {
            return defaultAutoCalibrate(indexVersion);
        }

        String value = node.toString();
        IvfAutoCalibrationProfile profile = Booleans.isBoolean(value)
            ? (Booleans.parseBoolean(value) ? defaultEnabledProfile(indexVersion) : IvfAutoCalibrationProfile.DISABLED)
            : IvfAutoCalibrationProfile.fromString(value)
                .orElseThrow(
                    () -> new IllegalArgumentException(
                        "'"
                            + NAME
                            + "' must be a boolean or one of "
                            + Arrays.toString(IvfAutoCalibrationProfile.values())
                            + ", got ["
                            + node
                            + "] for field ["
                            + fieldName
                            + "]"
                    )
                );

        return new AutoCalibrate(node, profile);
    }

    boolean enabled() {
        return profile != IvfAutoCalibrationProfile.DISABLED;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (originalValue != null) {
            builder.field(NAME, originalValue);
        }
        return builder;
    }
}
