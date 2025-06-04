/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.plugin.analysis.icu;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;
import com.ibm.icu.util.ULocale;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.plugin.analysis.icu.ICUCollationKeywordFieldMapper.CollationFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollationFieldTypeTests extends FieldTypeTestCase {

    private static final Collator DEFAULT_COLLATOR = Collator.getInstance(ULocale.ROOT).freeze();

    private static CollationFieldType createFieldType() {
        return new CollationFieldType("field", DEFAULT_COLLATOR);
    }

    public void testIsFieldWithinQuery() throws IOException {
        CollationFieldType ft = createFieldType();
        // current impl ignores args and shourd always return INTERSECTS
        assertEquals(
            Relation.INTERSECTS,
            ft.isFieldWithinQuery(
                null,
                RandomStrings.randomAsciiOfLengthBetween(random(), 0, 5),
                RandomStrings.randomAsciiOfLengthBetween(random(), 0, 5),
                randomBoolean(),
                randomBoolean(),
                null,
                null,
                null
            )
        );
    }

    public void testTermQuery() {
        Collator collator = Collator.getInstance(new ULocale("tr"));
        collator.setStrength(Collator.PRIMARY);
        collator.freeze();
        MappedFieldType ft = new CollationFieldType("field", collator);

        RawCollationKey key = collator.getRawCollationKey("ı will use turkish casıng", null);
        BytesRef expected = new BytesRef(key.bytes, 0, key.size);

        assertEquals(new TermQuery(new Term("field", expected)), ft.termQuery("I WİLL USE TURKİSH CASING", null));

        MappedFieldType unsearchable = new CollationFieldType("field", false, collator);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("bar", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        Collator collator = DEFAULT_COLLATOR;
        MappedFieldType ft = new CollationFieldType("field", collator);

        RawCollationKey fooKey = collator.getRawCollationKey("foo", null);
        RawCollationKey barKey = collator.getRawCollationKey("bar", null);

        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef(fooKey.bytes, 0, fooKey.size));
        terms.add(new BytesRef(barKey.bytes, 0, barKey.size));

        assertEquals(new TermInSetQuery("field", terms), ft.termsQuery(Arrays.asList("foo", "bar"), null));

        MappedFieldType unsearchable = new CollationFieldType("field", false, collator);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.termsQuery(Arrays.asList("foo", "bar"), null)
        );
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testRegexpQuery() {
        MappedFieldType ft = createFieldType();
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> ft.regexpQuery("foo.*", 0, 0, 10, null, randomMockContext())
        );
        assertEquals("[regexp] queries are not supported on [icu_collation_keyword] fields.", e.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = createFieldType();
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, randomMockContext())
        );
        assertEquals("[fuzzy] queries are not supported on [icu_collation_keyword] fields.", e.getMessage());
    }

    public void testPrefixQuery() {
        MappedFieldType ft = createFieldType();
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> ft.prefixQuery("prefix", null, randomMockContext())
        );
        assertEquals("[prefix] queries are not supported on [icu_collation_keyword] fields.", e.getMessage());
    }

    public void testWildcardQuery() {
        MappedFieldType ft = createFieldType();
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> ft.wildcardQuery("foo*", null, randomMockContext())
        );
        assertEquals("[wildcard] queries are not supported on [icu_collation_keyword] fields.", e.getMessage());
    }

    public void testRangeQuery() {
        MappedFieldType ft = createFieldType();
        RawCollationKey aKey = DEFAULT_COLLATOR.getRawCollationKey("a", null);
        RawCollationKey bKey = DEFAULT_COLLATOR.getRawCollationKey("b", null);

        TermRangeQuery expected = new TermRangeQuery(
            "field",
            new BytesRef(aKey.bytes, 0, aKey.size),
            new BytesRef(bKey.bytes, 0, bKey.size),
            false,
            false
        );

        assertEquals(expected, ft.rangeQuery("a", "b", false, false, null, null, null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.rangeQuery("a", "b", true, true, null, null, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when " + "'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );

        MappedFieldType unsearchable = new CollationFieldType("field", false, DEFAULT_COLLATOR);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.rangeQuery("a", "b", false, false, null, null, null, MOCK_CONTEXT)
        );
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }
}
