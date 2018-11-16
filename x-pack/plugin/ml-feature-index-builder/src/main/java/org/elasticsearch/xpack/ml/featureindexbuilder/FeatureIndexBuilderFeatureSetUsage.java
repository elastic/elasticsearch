/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackFeatureSet.Usage;

import java.io.IOException;

public class FeatureIndexBuilderFeatureSetUsage extends Usage {
    public FeatureIndexBuilderFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
    }

    public FeatureIndexBuilderFeatureSetUsage(boolean available, boolean enabled) {
        super(XPackField.DATA_FRAME, available, enabled);
    }
}
