/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.delayeddatacheck;

import java.util.Collections;
import java.util.List;

public class NullDelayedDataDetector implements DelayedDataDetector {

    @Override
    public List<DelayedDataDetectorFactory.BucketWithMissingData> detectMissingData(long unusedTimeStamp) {
        return Collections.emptyList();
    }

    @Override
    public long getWindow() {
        return 0;
    }

}
