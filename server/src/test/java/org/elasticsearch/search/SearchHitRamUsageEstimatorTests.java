/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class SearchHitRamUsageEstimatorTests extends ESTestCase {

    public void testHighlightFragmentsAreCounted() {
        String fragment = randomAlphaOfLength(2048);
        SearchHit plain = SearchHit.unpooled(1);
        SearchHit highlighted = SearchHit.unpooled(1);
        highlighted.highlightFields(Map.of("body", new HighlightField("body", new Text[] { new Text(fragment) })));

        long delta = highlighted.ramBytesUsed() - plain.ramBytesUsed();
        assertThat(delta, greaterThanOrEqualTo((long) fragment.length()));
    }

    public void testNullFragmentsAreTolerated() {
        SearchHit hit = SearchHit.unpooled(1);
        hit.highlightFields(Map.of("body", new HighlightField("body", null)));
        assertThat(hit.ramBytesUsed(), greaterThanOrEqualTo(0L));
    }

    public void testEstimatingDoesNotMaterializeTheOtherViewOfAFragment() {
        byte[] utf8 = randomAlphaOfLength(64).getBytes(StandardCharsets.UTF_8);
        Text fromBytes = new Text(new XContentString.UTF8Bytes(utf8, 0, utf8.length));
        Text fromString = new Text(randomAlphaOfLength(64));
        SearchHit hit = SearchHit.unpooled(1);
        hit.highlightFields(Map.of("body", new HighlightField("body", new Text[] { fromBytes, fromString })));

        hit.ramBytesUsed();

        assertFalse(fromBytes.hasString());
        assertFalse(fromString.hasBytes());
    }
}
