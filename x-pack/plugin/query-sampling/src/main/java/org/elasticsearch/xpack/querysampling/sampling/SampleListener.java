/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

/**
 * Hook for what to do with a picked query. Listeners run in turn on the pipeline's thread, so they must not block;
 * a listener that throws does not prevent the others from being called.
 */
public interface SampleListener {

    void onSampled(SampledQuery query);
}
