/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.autoscaling.model;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Map;

public class MetricsContainer extends NamedValuesContainer {

    public MetricsContainer(final Map<String, Integer> valueMap) {
        super(valueMap);
    }

    public MetricsContainer(StreamInput input) throws IOException {
        super(input);
    }

}
