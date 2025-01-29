/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;

public class InputTypeTests extends ESTestCase {
    public static InputType randomWithoutUnspecified() {
        return randomFrom(InputType.INGEST, InputType.SEARCH, InputType.CLUSTERING, InputType.CLASSIFICATION);
    }

    public static InputType randomWithIngestAndSearch() {
        return randomFrom(InputType.INGEST, InputType.SEARCH);
    }
}
