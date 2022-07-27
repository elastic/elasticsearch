/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.Defaults;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.PrefixFieldType;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.SearchAsYouTypeFieldType;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.ShingleFieldType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_REWRITE;
import static org.hamcrest.Matchers.equalTo;

public class SearchAsYouTypeFieldTypeTests extends FieldTypeTestCase {

    private static final String NAME = "a_field";
    private static final FieldType UNSEARCHABLE = new FieldType();
    static {
        UNSEARCHABLE.setIndexOptions(IndexOptions.NONE);
        UNSEARCHABLE.freeze();
    }

    private static final FieldType SEARCHABLE = new FieldType();
    static {
        SEARCHABLE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        SEARCHABLE.freeze();
    }

    private static SearchAsYouTypeFieldType createFieldType() {
        final SearchAsYouTypeFieldType fieldType = new SearchAsYouTypeFieldType(
            NAME,
            SEARCHABLE,
            null,
            Lucene.STANDARD_ANALYZER,
            Lucene.STANDARD_ANALYZER,
            Collections.emptyMap()
        );
        fieldType.setPrefixField(new PrefixFieldType(NAME, TextSearchInfo.SIMPLE_MATCH_ONLY, Defaults.MIN_GRAM, Defaults.MAX_GRAM));
        fieldType.setShingleFields(new ShingleFieldType[] { new ShingleFieldType(fieldType.name(), 2, TextSearchInfo.SIMPLE_MATCH_ONLY) });
        return fieldType;
    }

    public void testTermQuery() {
        final MappedFieldType fieldType = createFieldType();

        assertThat(fieldType.termQuery("foo", null), equalTo(new TermQuery(new Term(NAME, "foo"))));

        SearchAsYouTypeFieldType unsearchable = new SearchAsYouTypeFieldType(
            NAME,
            UNSEARCHABLE,
            null,
            Lucene.STANDARD_ANALYZER,
            Lucene.STANDARD_ANALYZER,
            Collections.emptyMap()
        );
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("foo", null));
        assertThat(e.getMessage(), equalTo("Cannot search on field [" + NAME + "] since it is not indexed."));
    }

    public void testTermsQuery() {
        final MappedFieldType fieldType = createFieldType();

        assertThat(
            fieldType.termsQuery(asList("foo", "bar"), null),
            equalTo(new TermInSetQuery(NAME, asList(new BytesRef("foo"), new BytesRef("bar"))))
        );

        SearchAsYouTypeFieldType unsearchable = new SearchAsYouTypeFieldType(
            NAME,
            UNSEARCHABLE,
            null,
            Lucene.STANDARD_ANALYZER,
            Lucene.STANDARD_ANALYZER,
            Collections.emptyMap()
        );
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.termsQuery(asList("foo", "bar"), null)
        );
        assertThat(e.getMessage(), equalTo("Cannot search on field [" + NAME + "] since it is not indexed."));
    }

    public void testPrefixQuery() {
        final SearchAsYouTypeFieldType fieldType = createFieldType();

        // this term should be a length that can be rewriteable to a term query on the prefix field
        final String withinBoundsTerm = "foo";
        assertThat(
            fieldType.prefixQuery(withinBoundsTerm, CONSTANT_SCORE_REWRITE, randomMockContext()),
            equalTo(new ConstantScoreQuery(new TermQuery(new Term(NAME + "._index_prefix", withinBoundsTerm))))
        );

        // our defaults don't allow a situation where a term can be too small

        // this term should be too long to be rewriteable to a term query on the prefix field
        final String longTerm = "toolongforourprefixfieldthistermis";
        assertThat(
            fieldType.prefixQuery(longTerm, CONSTANT_SCORE_REWRITE, MOCK_CONTEXT),
            equalTo(new PrefixQuery(new Term(NAME, longTerm)))
        );

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> fieldType.prefixQuery(longTerm, CONSTANT_SCORE_REWRITE, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. "
                + "For optimised prefix queries on text fields please enable [index_prefixes].",
            ee.getMessage()
        );
    }

    public void testFetchSourceValue() throws IOException {
        SearchAsYouTypeFieldType fieldType = createFieldType();

        assertEquals(List.of("value"), fetchSourceValue(fieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(fieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(fieldType, true));

        SearchAsYouTypeFieldMapper.PrefixFieldType prefixFieldType = new SearchAsYouTypeFieldMapper.PrefixFieldType(
            fieldType.name(),
            fieldType.getTextSearchInfo(),
            2,
            10
        );
        assertEquals(List.of("value"), fetchSourceValue(prefixFieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(prefixFieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(prefixFieldType, true));

        SearchAsYouTypeFieldMapper.ShingleFieldType shingleFieldType = new SearchAsYouTypeFieldMapper.ShingleFieldType(
            fieldType.name(),
            5,
            fieldType.getTextSearchInfo()
        );
        assertEquals(List.of("value"), fetchSourceValue(shingleFieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(shingleFieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(shingleFieldType, true));
    }
}
