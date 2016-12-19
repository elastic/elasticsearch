/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
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
