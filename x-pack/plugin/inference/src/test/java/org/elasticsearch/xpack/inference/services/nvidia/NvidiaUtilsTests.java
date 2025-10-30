/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class NvidiaUtilsTests extends ESTestCase {

    public void testInputTypeToString_Search() {
        assertThat(NvidiaUtils.inputTypeToString(InputType.SEARCH), is("query"));
    }

    public void testInputTypeToString_InternalSearch() {
        assertThat(NvidiaUtils.inputTypeToString(InputType.INTERNAL_SEARCH), is("query"));
    }

    public void testInputTypeToString_Ingest() {
        assertThat(NvidiaUtils.inputTypeToString(InputType.INGEST), is("passage"));
    }

    public void testInputTypeToString_InternalIngest() {
        assertThat(NvidiaUtils.inputTypeToString(InputType.INTERNAL_INGEST), is("passage"));
    }

    public void testInputTypeToString_Null() {
        assertNull(NvidiaUtils.inputTypeToString(null));
    }

    public void testInputTypeToString_Unspecified() {
        AssertionError assertionError = assertThrows(AssertionError.class, () -> NvidiaUtils.inputTypeToString(InputType.UNSPECIFIED));
        assertThat(assertionError.getMessage(), is("received invalid input type value [unspecified]"));
    }
}
