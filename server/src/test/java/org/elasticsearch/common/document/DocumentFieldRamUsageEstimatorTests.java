/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.document;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class DocumentFieldRamUsageEstimatorTests extends ESTestCase {

    public void testEstimateIsNonZeroForEmptyField() {
        DocumentField field = new DocumentField("name", Collections.emptyList());
        assertThat(DocumentFieldRamUsageEstimator.estimate(field), greaterThan(0L));
    }

    public void testEstimateGrowsWithStringValueSize() {
        DocumentField small = new DocumentField("f", List.of("hi"));
        String largeValue = randomAlphaOfLength(4096);
        DocumentField large = new DocumentField("f", List.of(largeValue));

        long smallEstimate = DocumentFieldRamUsageEstimator.estimate(small);
        long largeEstimate = DocumentFieldRamUsageEstimator.estimate(large);
        long expectedCharPayload = (long) largeValue.length() * Character.BYTES;
        assertThat(largeEstimate - smallEstimate, greaterThanOrEqualTo(expectedCharPayload - 16L));
    }

    public void testEstimateGrowsWithCollectionSize() {
        List<Object> manyValues = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            manyValues.add("entry-" + i);
        }
        DocumentField large = new DocumentField("f", manyValues);
        DocumentField small = new DocumentField("f", List.of("one"));
        assertThat(DocumentFieldRamUsageEstimator.estimate(large), greaterThan(DocumentFieldRamUsageEstimator.estimate(small)));
    }

    public void testEstimateAccountsForNestedMapValues() {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("text", randomAlphaOfLength(64));
            entry.put("number", i);
            values.add(entry);
        }
        DocumentField nested = new DocumentField("f", values);
        DocumentField shallow = new DocumentField("f", List.of("just a small string"));
        assertThat(DocumentFieldRamUsageEstimator.estimate(nested), greaterThan(DocumentFieldRamUsageEstimator.estimate(shallow)));
    }

    public void testEstimateIncludesIgnoredValues() {
        DocumentField withoutIgnored = new DocumentField("f", List.of("v"), Collections.emptyList());
        DocumentField withIgnored = new DocumentField("f", List.of("v"), List.of(randomAlphaOfLength(2048)));
        assertThat(
            DocumentFieldRamUsageEstimator.estimate(withIgnored),
            greaterThan(DocumentFieldRamUsageEstimator.estimate(withoutIgnored))
        );
    }
}
