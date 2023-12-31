/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import java.util.Collections;
import java.util.Date;

public class MlDataRemoverTests extends ESTestCase {
    public void testStringOrNull() {
        MlDataRemover remover = (requestsPerSecond, listener, isTimedOutSupplier) -> {};

        SearchHitBuilder hitBuilder = new SearchHitBuilder(0);
        var hit = hitBuilder.build();
        try {
            assertNull(remover.stringFieldValueOrNull(hit, "missing"));
        } finally {
            hit.decRef();
        }

        hitBuilder = new SearchHitBuilder(0);
        hitBuilder.addField("not_a_string", Collections.singletonList(new Date()));
        var hit2 = hitBuilder.build();
        try {
            assertNull(remover.stringFieldValueOrNull(hit2, "not_a_string"));
        } finally {
            hit2.decRef();
        }

        hitBuilder = new SearchHitBuilder(0);
        hitBuilder.addField("string_field", Collections.singletonList("actual_string_value"));
        var hit3 = hitBuilder.build();
        try {
            assertEquals("actual_string_value", remover.stringFieldValueOrNull(hit3, "string_field"));
        } finally {
            hit3.decRef();
        }
    }
}
