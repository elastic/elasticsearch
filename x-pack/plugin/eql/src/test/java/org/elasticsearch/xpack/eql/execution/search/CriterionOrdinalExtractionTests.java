/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.assembler.BoxedQueryRequest;
import org.elasticsearch.xpack.eql.execution.assembler.Criterion;
import org.elasticsearch.xpack.eql.execution.search.extractor.FieldHitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

public class CriterionOrdinalExtractionTests extends ESTestCase {
    private String tsField = "timestamp";
    private String tbField = "tiebreaker";

    private HitExtractor tsExtractor = new FieldHitExtractor(tsField, tsField, DataTypes.LONG, null, true, null, false);
    private HitExtractor tbExtractor = new FieldHitExtractor(tbField, tbField, DataTypes.LONG, null, true, null, false);

    public void testTimeOnly() throws Exception {
        long time = randomLong();
        Ordinal ordinal = ordinal(searchHit(time, null), false);
        assertEquals(time, ordinal.timestamp());
        assertNull(ordinal.tiebreaker());
    }

    public void testTimeAndTiebreaker() throws Exception {
        long time = randomLong();
        long tb = randomLong();
        Ordinal ordinal = ordinal(searchHit(time, tb), true);
        assertEquals(time, ordinal.timestamp());
        assertEquals(tb, ordinal.tiebreaker());
    }

    public void testTimeAndTiebreakerNull() throws Exception {
        long time = randomLong();
        Ordinal ordinal = ordinal(searchHit(time, null), true);
        assertEquals(time, ordinal.timestamp());
        assertNull(ordinal.tiebreaker());
    }

    public void testTimeNotComparable() throws Exception {
        HitExtractor badExtractor = new FieldHitExtractor(tsField, tsField, DataTypes.BINARY, null, true, null, false);
        SearchHit hit = searchHit(randomAlphaOfLength(10), null);
        Criterion<BoxedQueryRequest> criterion = new Criterion<BoxedQueryRequest>(0, null, emptyList(), badExtractor, null, false);
        EqlIllegalArgumentException exception = expectThrows(EqlIllegalArgumentException.class, () -> criterion.ordinal(hit));
        assertTrue(exception.getMessage().startsWith("Expected timestamp"));
    }

    public void testTiebreakerNotComparable() throws Exception {
        final Object o = randomDateTimeZone();
        HitExtractor badExtractor = new HitExtractor() {
            @Override
            public Object extract(SearchHit hit) {
                return o;
            }

            @Override
            public String hitName() {
                return null;
            }

            @Override
            public String getWriteableName() {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
            }
        };
        SearchHit hit = searchHit(randomLong(), o);
        Criterion<BoxedQueryRequest> criterion = new Criterion<BoxedQueryRequest>(0, null, emptyList(), tsExtractor, badExtractor, false);
        EqlIllegalArgumentException exception = expectThrows(EqlIllegalArgumentException.class, () -> criterion.ordinal(hit));
        assertTrue(exception.getMessage().startsWith("Expected tiebreaker"));
    }

    private SearchHit searchHit(Object timeValue, Object tiebreakerValue) {
        Map<String, DocumentField> fields = new HashMap<>();
        fields.put(tsField, new DocumentField(tsField, singletonList(timeValue)));
        fields.put(tbField, new DocumentField(tsField, singletonList(tiebreakerValue)));

        return new SearchHit(randomInt(), randomAlphaOfLength(10), fields, emptyMap());
    }

    private Ordinal ordinal(SearchHit hit, boolean withTiebreaker) {
        return new Criterion<BoxedQueryRequest>(0, null, emptyList(), tsExtractor, withTiebreaker ? tbExtractor : null, false).ordinal(hit);
    }
}
