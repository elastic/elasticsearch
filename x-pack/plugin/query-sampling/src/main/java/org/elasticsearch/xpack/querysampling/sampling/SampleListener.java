/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

/**
 * The hook for whatever wants to do something with the sample. Capturing, counting repeats and deciding
 * which queries join the sample is common to every use of the framework; what to do with a query once it
 * was picked is not, and is up to the listeners.
 * <p>
 * Listeners are called one after the other on the pipeline's thread, so they must be quick and must not
 * block: hand the work over to another executor if there is more to do than keeping the query somewhere.
 * A listener that throws does not prevent the others from being called.
 */
public interface SampleListener {

    void onSampled(SampledQuery query);
}
