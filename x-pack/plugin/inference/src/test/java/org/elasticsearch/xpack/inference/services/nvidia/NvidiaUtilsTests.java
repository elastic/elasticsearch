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

    private static final String PASSAGE_VALUE = "passage";
    private static final String QUERY_VALUE = "query";

    public void testInputTypeToString_Search() {
        assertThat(NvidiaUtils.inputTypeToString(InputType.SEARCH), is(QUERY_VALUE));
    }

    public void testInputTypeToString_InternalSearch() {
        assertThat(NvidiaUtils.inputTypeToString(InputType.INTERNAL_SEARCH), is(QUERY_VALUE));
    }

    public void testInputTypeToString_Ingest() {
        assertThat(NvidiaUtils.inputTypeToString(InputType.INGEST), is(PASSAGE_VALUE));
    }

    public void testInputTypeToString_InternalIngest() {
        assertThat(NvidiaUtils.inputTypeToString(InputType.INTERNAL_INGEST), is(PASSAGE_VALUE));
    }

    public void testInputTypeToString_Null() {
        IllegalArgumentException assertionError = assertThrows(IllegalArgumentException.class, () -> NvidiaUtils.inputTypeToString(null));
        assertThat(
            assertionError.getMessage(),
            is("Unrecognized input_type [null], must be one of [ingest, search, internal_search, internal_ingest]")
        );
    }

    public void testInputTypeToString_Unspecified() {
        IllegalArgumentException assertionError = assertThrows(
            IllegalArgumentException.class,
            () -> NvidiaUtils.inputTypeToString(InputType.UNSPECIFIED)
        );
        assertThat(
            assertionError.getMessage(),
            is("Unrecognized input_type [unspecified], must be one of [ingest, search, internal_search, internal_ingest]")
        );
    }
}
