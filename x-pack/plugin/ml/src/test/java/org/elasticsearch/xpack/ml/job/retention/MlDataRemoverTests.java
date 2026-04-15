/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.core.ReleasableRef;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import java.util.Collections;
import java.util.Date;

public class MlDataRemoverTests extends ESTestCase {
    public void testStringOrNull() {
        MlDataRemover remover = (requestsPerSecond, listener, isTimedOutSupplier) -> {};

        SearchHitBuilder hitBuilder = new SearchHitBuilder(0);
        SearchHit h0 = hitBuilder.build();
        try (var h0Ref = ReleasableRef.of(h0)) {
            assertNull(remover.stringFieldValueOrNull(h0Ref.get(), "missing"));
        }

        hitBuilder = new SearchHitBuilder(0);
        hitBuilder.addField("not_a_string", Collections.singletonList(new Date()));
        SearchHit h1 = hitBuilder.build();
        try (var h1Ref = ReleasableRef.of(h1)) {
            assertNull(remover.stringFieldValueOrNull(h1Ref.get(), "not_a_string"));
        }

        hitBuilder = new SearchHitBuilder(0);
        hitBuilder.addField("string_field", Collections.singletonList("actual_string_value"));
        SearchHit h2 = hitBuilder.build();
        try (var h2Ref = ReleasableRef.of(h2)) {
            assertEquals("actual_string_value", remover.stringFieldValueOrNull(h2Ref.get(), "string_field"));
        }
    }
}
