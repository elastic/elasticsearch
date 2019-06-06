/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.graph;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;

public class GraphFeatureSetUsage extends XPackFeatureSet.Usage {

    public GraphFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
    }

    public GraphFeatureSetUsage(boolean available, boolean enabled) {
        super(XPackField.GRAPH, available, enabled);
    }
}
