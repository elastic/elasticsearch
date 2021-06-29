/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.benchmark.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Stores measurement samples.
 *
 * This class is NOT threadsafe.
 */
public final class SampleRecorder {
    private final List<Sample> samples;

    public SampleRecorder(int iterations) {
        this.samples = new ArrayList<>(iterations);
    }

    public void addSample(Sample sample) {
        samples.add(sample);
    }

    public List<Sample> getSamples() {
        return Collections.unmodifiableList(samples);
    }
}
