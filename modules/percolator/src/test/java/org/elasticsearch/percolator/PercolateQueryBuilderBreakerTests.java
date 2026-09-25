/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.percolator;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;
import java.util.List;

/**
 * Circuit-breaker estimate tests for {@link PercolateQueryBuilder}. Kept separate from
 * {@link PercolateQueryBuilderTests} because that class exercises
 * {@link PercolateQueryBuilder#createMultiDocumentSearcher} and accumulates Lucene codec
 * field-name mappings across iterations. Running hundreds of iterations (flakiness detection)
 * would push the mapping count past the {@code RandomCodec} safety limit of 10,000.
 */
public class PercolateQueryBuilderBreakerTests extends ESTestCase {

    public void testInlineDocumentsEstimate() {
        // Formula: BASELINE + field.length()*2+64 + docs.size()*8 + sum(doc.length())
        String field = "myfield";
        BytesReference doc = new BytesArray("{\"k\":\"v\"}"); // 9 bytes

        PercolateQueryBuilder single = new PercolateQueryBuilder(field, Collections.singletonList(doc), XContentType.JSON);
        long expected = AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES + field.length() * 2L + 64L + 1 * 8L + doc.length();
        assertEquals(expected, single.parseTimeBreakerEstimate());

        BytesReference doc2 = new BytesArray("{\"k2\":\"v2\"}"); // 11 bytes
        PercolateQueryBuilder multi = new PercolateQueryBuilder(field, List.of(doc, doc2), XContentType.JSON);
        long expectedMulti = AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES + field.length() * 2L + 64L + 2 * 8L + doc.length()
            + doc2.length();
        assertEquals(expectedMulti, multi.parseTimeBreakerEstimate());
    }

    public void testIndexedDocumentEstimate() {
        // The indexed-document ctor sets documents = Collections.emptyList(), so
        // parseTimeBreakerEstimate() must enter the isEmpty() branch and charge metadata strings.
        String field = "myfield";
        String index = "my-index";
        String id = "my-id";
        String routing = "my-routing";
        String preference = "my-preference";

        PercolateQueryBuilder withAll = new PercolateQueryBuilder(field, index, id, routing, preference, -1L);
        long expected = AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES + field.length() * 2L + 64L + index.length() * 2L + 64L + id
            .length() * 2L + 64L + routing.length() * 2L + 64L + preference.length() * 2L + 64L;
        assertEquals(expected, withAll.parseTimeBreakerEstimate());

        // Null optional strings (routing, preference) must not be charged.
        PercolateQueryBuilder minimal = new PercolateQueryBuilder(field, index, id, null, null, -1L);
        long expectedMinimal = AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES + field.length() * 2L + 64L + index.length() * 2L
            + 64L + id.length() * 2L + 64L;
        assertEquals(expectedMinimal, minimal.parseTimeBreakerEstimate());
    }
}
