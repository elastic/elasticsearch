/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.CoreMatchers.is;

public class InputTypeTests extends ESTestCase {
    public static InputType randomWithoutUnspecified() {
        return randomFrom(
            InputType.INGEST,
            InputType.SEARCH,
            InputType.CLUSTERING,
            InputType.CLASSIFICATION,
            InputType.INTERNAL_SEARCH,
            InputType.INTERNAL_INGEST
        );
    }

    public static InputType randomWithNull() {
        return randomBoolean()
            ? null
            : randomFrom(
                InputType.UNSPECIFIED,
                InputType.INGEST,
                InputType.SEARCH,
                InputType.CLUSTERING,
                InputType.CLASSIFICATION,
                InputType.INTERNAL_SEARCH,
                InputType.INTERNAL_INGEST
            );
    }

    public static InputType randomSearchAndIngestWithNull() {
        return randomBoolean()
            ? null
            : randomFrom(InputType.UNSPECIFIED, InputType.INGEST, InputType.SEARCH, InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST);
    }

    public static InputType randomSearchAndIngestWithNullWithoutUnspecified() {
        return randomBoolean()
            ? null
            : randomFrom(InputType.INGEST, InputType.SEARCH, InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST);
    }

    public static InputType randomWithIngestAndSearch() {
        return randomFrom(InputType.INGEST, InputType.SEARCH, InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST);
    }

    public static InputType randomWithInternalAndUnspecified() {
        return randomFrom(InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST, InputType.UNSPECIFIED);
    }

    public void testFromRestString_ValidInputType() {
        for (String internal : List.of("search", "ingest", "classification", "clustering", "unspecified")) {
            assertEquals(InputType.fromRestString(internal), InputType.fromString(internal));
        }
    }

    public void testFromRestString_ThrowsErrorForInternalInputTypes() {
        for (String internal : List.of("internal_ingest", "internal_search")) {
            var thrownException = expectThrows(IllegalArgumentException.class, () -> InputType.fromRestString(internal));

            assertThat(thrownException.getMessage(), is(format("Unrecognized input_type [%s]", internal)));
        }
    }

    public void testFromRestString_ThrowsErrorForInvalidInputTypes() {
        var thrownException = expectThrows(IllegalArgumentException.class, () -> InputType.fromRestString("foo"));

        assertThat(thrownException.getMessage(), is("No enum constant org.elasticsearch.inference.InputType.FOO"));
    }
}
