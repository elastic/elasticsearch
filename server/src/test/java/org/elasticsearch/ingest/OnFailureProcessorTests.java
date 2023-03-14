/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OnFailureProcessorTests extends ESTestCase {

    public void testOnFailureProcessor() {
        TestProcessor processor = new TestProcessor(new RuntimeException("error"));
        OnFailureProcessor onFailureProcessor = new OnFailureProcessor(false, processor, List.of());

        assertThat(onFailureProcessor.isIgnoreFailure(), is(false));

        assertThat(onFailureProcessor.getProcessors().size(), equalTo(1));
        assertThat(onFailureProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(onFailureProcessor.getInnerProcessor(), sameInstance(processor));

        assertThat(onFailureProcessor.getOnFailureProcessors().isEmpty(), is(true));

        assertThat(onFailureProcessor.getInnerMetric(), sameInstance(onFailureProcessor.getProcessorsWithMetrics().get(0).v2()));
    }
}
