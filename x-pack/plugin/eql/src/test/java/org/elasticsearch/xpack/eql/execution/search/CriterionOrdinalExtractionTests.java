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
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.assembler.SequenceCriterion;
import org.elasticsearch.xpack.eql.execution.search.extractor.FieldHitExtractor;
import org.elasticsearch.xpack.eql.execution.search.extractor.ImplicitTiebreakerHitExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.eql.EqlTestUtils.randomSearchLongSortValues;
import static org.elasticsearch.xpack.eql.EqlTestUtils.randomSearchSortValues;
import static org.elasticsearch.xpack.eql.execution.search.OrdinalTests.randomTimestamp;
import static org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor.MultiValueSupport.FULL;

public class CriterionOrdinalExtractionTests extends ESTestCase {
    private String tsField = "timestamp";
    private String tbField = "tiebreaker";

    private HitExtractor tsExtractor = new FieldHitExtractor(tsField, DataTypes.LONG, null, null, FULL);
    private HitExtractor tbExtractor = new FieldHitExtractor(tbField, DataTypes.LONG, null, null, FULL);
    private HitExtractor implicitTbExtractor = ImplicitTiebreakerHitExtractor.INSTANCE;

    public void testTimeOnly() throws Exception {
        Object time = randomTimestamp();
        long implicitTbValue = randomLong();
        Ordinal ordinal = ordinal(searchHit(time, null, new Object[] { implicitTbValue }), false);
        assertEquals(time, ordinal.timestamp());
        assertNull(ordinal.tiebreaker());
        assertEquals(implicitTbValue, ordinal.implicitTiebreaker());
    }

    public void testTimeAndTiebreaker() throws Exception {
        Object time = randomTimestamp();
        long tb = randomLong();
        long implicitTbValue = randomLong();
        Ordinal ordinal = ordinal(searchHit(time, tb, new Object[] { implicitTbValue }), true);
        assertEquals(time, ordinal.timestamp());
        assertEquals(tb, ordinal.tiebreaker());
        assertEquals(implicitTbValue, ordinal.implicitTiebreaker());
    }

    public void testTimeAndTiebreakerNull() throws Exception {
        Object time = randomTimestamp();
        Ordinal ordinal = ordinal(searchHit(time, null), true);
        assertEquals(time, ordinal.timestamp());
        assertNull(ordinal.tiebreaker());
    }

    public void testTimeNotComparable() throws Exception {
        HitExtractor badExtractor = new FieldHitExtractor(tsField, DataTypes.BINARY, null, null, FULL);
        SearchHit hit = searchHit(randomAlphaOfLength(10), null);
        SequenceCriterion criterion = new SequenceCriterion(0, null, emptyList(), badExtractor, null, null, false, false);
        EqlIllegalArgumentException exception = expectThrows(EqlIllegalArgumentException.class, () -> criterion.ordinal(hit));
        assertTrue(exception.getMessage().startsWith("Expected timestamp"));
    }

    public void testImplicitTiebreakerMissing() throws Exception {
        SearchHit hit = searchHit(randomTimestamp(), null, new Object[0]);
        SequenceCriterion criterion = new SequenceCriterion(
            0,
            null,
            emptyList(),
            tsExtractor,
            null,
            implicitTbExtractor,
            randomBoolean(),
            randomBoolean()
        );
        EqlIllegalArgumentException exception = expectThrows(EqlIllegalArgumentException.class, () -> criterion.ordinal(hit));
        assertTrue(exception.getMessage().startsWith("Expected at least one sorting value in the search hit, but got none"));
    }

    public void testImplicitTiebreakerNotANumber() throws Exception {
        SearchHit hit = searchHit(randomTimestamp(), null, new Object[] { "test string" });
        SequenceCriterion criterion = new SequenceCriterion(
            0,
            null,
            emptyList(),
            tsExtractor,
            null,
            implicitTbExtractor,
            randomBoolean(),
            randomBoolean()
        );
        EqlIllegalArgumentException exception = expectThrows(EqlIllegalArgumentException.class, () -> criterion.ordinal(hit));
        assertTrue(exception.getMessage().startsWith("Expected _shard_doc/implicit tiebreaker as long but got [test string]"));
    }

    public void testTiebreakerNotComparable() throws Exception {
        final Object o = randomZone();
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
            public void writeTo(StreamOutput out) throws IOException {}
        };
        SearchHit hit = searchHit(randomTimestamp(), o);
        SequenceCriterion criterion = new SequenceCriterion(
            0,
            null,
            emptyList(),
            tsExtractor,
            badExtractor,
            implicitTbExtractor,
            false,
            false
        );
        EqlIllegalArgumentException exception = expectThrows(EqlIllegalArgumentException.class, () -> criterion.ordinal(hit));
        assertTrue(exception.getMessage().startsWith("Expected tiebreaker"));
    }

    private SearchHit searchHit(Object timeValue, Object tiebreakerValue) {
        return searchHit(timeValue, tiebreakerValue, () -> randomSearchLongSortValues());
    }

    private SearchHit searchHit(Object timeValue, Object tiebreakerValue, Object[] implicitTiebreakerValues) {
        return searchHit(timeValue, tiebreakerValue, () -> randomSearchSortValues(implicitTiebreakerValues));
    }

    private SearchHit searchHit(Object timeValue, Object tiebreakerValue, Supplier<SearchSortValues> searchSortValues) {
        Map<String, DocumentField> fields = new HashMap<>();
        fields.put(tsField, new DocumentField(tsField, singletonList(timeValue)));
        fields.put(tbField, new DocumentField(tsField, singletonList(tiebreakerValue)));
        SearchHit searchHit = SearchHit.unpooled(randomInt(), randomAlphaOfLength(10));
        searchHit.addDocumentFields(fields, Map.of());
        searchHit.sortValues(searchSortValues.get());

        return searchHit;
    }

    private Ordinal ordinal(SearchHit hit, boolean withTiebreaker) {
        return new SequenceCriterion(
            0,
            null,
            emptyList(),
            tsExtractor,
            withTiebreaker ? tbExtractor : null,
            implicitTbExtractor,
            false,
            false
        ).ordinal(hit);
    }
}
