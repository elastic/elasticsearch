/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField.UnknownAnalyzer;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;

/**
 * Tests {@link Match#canSearchWithoutShards()}: the searches of an indexed field that return what Lucene does when they
 * are evaluated over the values of the field, on a node that has no shards to query.
 */
public class MatchSearchWithoutShardsTests extends ESTestCase {

    public void testTextWithKnownAnalyzer() {
        assertTrue(match(text("english", UnknownAnalyzer.NONE)).canSearchWithoutShards());
    }

    /**
     * Without the analyzer of the mapping the values can't be analyzed like the index does. It is not reported for
     * {@code semantic_text} either, which Lucene doesn't match by its terms at all.
     */
    public void testTextWithUnknownAnalyzer() {
        for (UnknownAnalyzer unknown : UnknownAnalyzer.values()) {
            if (unknown != UnknownAnalyzer.NONE) {
                assertFalse(unknown.toString(), match(text(null, unknown)).canSearchWithoutShards());
            }
        }
    }

    public void testKeyword() {
        assertTrue(match(keyword(false)).canSearchWithoutShards());
        // Lucene normalizes the query like the values it indexed, the values in the page are the original ones
        assertFalse(match(keyword(true)).canSearchWithoutShards());
    }

    /** Lucene parses the query with the mapper of the field, e.g. a date matches to the precision of the query. */
    public void testOtherTypes() {
        EsField date = DateEsField.dateEsField("f", Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        assertFalse(new Match(Source.EMPTY, field(date), of("2023-01-01"), null).canSearchWithoutShards());
        EsField integer = new EsField("f", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        assertFalse(new Match(Source.EMPTY, field(integer), of(1), null).canSearchWithoutShards());
    }

    /** A column that is not an indexed field is a runtime search, it never needs the shards. */
    public void testRuntimeSearch() {
        Match match = match(new ReferenceAttribute(Source.EMPTY, "f", DataType.KEYWORD));
        assertTrue(match.isRuntimeSearch());
        assertFalse(match.canSearchWithoutShards());
    }

    private static Match match(Expression field) {
        return new Match(Source.EMPTY, field, of("fox"), null);
    }

    private static FieldAttribute text(String analyzer, UnknownAnalyzer unknown) {
        return field(
            new TextEsField(
                "f",
                Map.of(),
                false,
                false,
                EsField.TimeSeriesFieldType.NONE,
                analyzer,
                TextEsField.DEFAULT_POSITION_INCREMENT_GAP,
                unknown,
                null
            )
        );
    }

    private static FieldAttribute keyword(boolean normalized) {
        return field(new KeywordEsField("f", Map.of(), true, Short.MAX_VALUE, normalized, false, EsField.TimeSeriesFieldType.NONE));
    }

    private static FieldAttribute field(EsField field) {
        return new FieldAttribute(Source.EMPTY, "f", field);
    }
}
