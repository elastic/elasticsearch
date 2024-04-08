/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.core.Tuple;

import java.util.List;

/**
 * A wrapping processor that adds failure handling logic around the wrapped processor.
 */
public class OnFailureProcessor extends CompoundProcessor implements WrappingProcessor {

    static final String TYPE = "on_failure";

    public OnFailureProcessor(boolean ignoreFailure, Processor processor, List<Processor> onFailureProcessors) {
        super(ignoreFailure, List.of(processor), onFailureProcessors);
    }

    @Override
    public Processor getInnerProcessor() {
        List<Processor> processors = this.getProcessors();
        assert processors.size() == 1;
        return processors.get(0);
    }

    IngestMetric getInnerMetric() {
        List<Tuple<Processor, IngestMetric>> processorsAndMetrics = this.getProcessorsWithMetrics();
        assert processorsAndMetrics.size() == 1;
        return processorsAndMetrics.get(0).v2();
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
