/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;

import static org.hamcrest.Matchers.is;

public class TransportCoordinatedInferenceActionTests extends ESTestCase {
    public void testConvertPrefixToInputType_ConvertsIngestCorrectly() {
        assertThat(
            TransportCoordinatedInferenceAction.convertPrefixToInputType(TrainedModelPrefixStrings.PrefixType.INGEST),
            is(InputType.INTERNAL_INGEST)
        );
    }

    public void testConvertPrefixToInputType_ConvertsSearchCorrectly() {
        assertThat(
            TransportCoordinatedInferenceAction.convertPrefixToInputType(TrainedModelPrefixStrings.PrefixType.SEARCH),
            is(InputType.INTERNAL_SEARCH)
        );
    }

    public void testConvertPrefixToInputType_DefaultsToIngestWhenUnknown() {
        assertThat(
            TransportCoordinatedInferenceAction.convertPrefixToInputType(TrainedModelPrefixStrings.PrefixType.NONE),
            is(InputType.INTERNAL_INGEST)
        );
    }
}
