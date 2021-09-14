/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.DefaultDetectorDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;

public class DefaultDetectorDescriptionTests extends ESTestCase {


    public void testOf_GivenOnlyFunctionAndFieldName() {
        Detector detector = new Detector.Builder("min", "value").build();

        assertEquals("min(value)", DefaultDetectorDescription.of(detector));
    }


    public void testOf_GivenOnlyFunctionAndFieldNameWithNonWordChars() {
        Detector detector = new Detector.Builder("min", "val-ue").build();

        assertEquals("min(\"val-ue\")", DefaultDetectorDescription.of(detector));
    }


    public void testOf_GivenFullyPopulatedDetector() {
        Detector.Builder detector = new Detector.Builder("sum", "value");
        detector.setByFieldName("airline");
        detector.setOverFieldName("region");
        detector.setUseNull(true);
        detector.setPartitionFieldName("planet");
        detector.setExcludeFrequent(Detector.ExcludeFrequent.ALL);

        assertEquals("sum(value) by airline over region usenull=true partitionfield=planet excludefrequent=all",
                DefaultDetectorDescription.of(detector.build()));
    }
}
