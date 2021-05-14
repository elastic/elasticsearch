/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import java.util.Collections;
import java.util.List;

/**
 * This class will always return an {@link Collections#emptyList()}.
 */
public class NullDelayedDataDetector implements DelayedDataDetector {

    /**
     * Always returns an empty collection
     * @param unusedTimeStamp unused Parameter
     * @return {@link Collections#emptyList()}
     */
    @Override
    public List<DelayedDataDetectorFactory.BucketWithMissingData> detectMissingData(long unusedTimeStamp) {
        return Collections.emptyList();
    }

    /**
     * Always returns 0
     * @return a 0
     */
    @Override
    public long getWindow() {
        return 0L;
    }

}
