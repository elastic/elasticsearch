/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.xpack.ml.job.process.normalizer.output.NormalizerResultHandler;
import org.elasticsearch.xpack.ml.process.NativeProcess;

/**
 * Interface representing the native C++ normalizer process
 */
public interface NormalizerProcess extends NativeProcess {

    /**
     * Create a result handler for this process's results.
     * @return results handler
     */
    NormalizerResultHandler createNormalizedResultsHandler();
}
