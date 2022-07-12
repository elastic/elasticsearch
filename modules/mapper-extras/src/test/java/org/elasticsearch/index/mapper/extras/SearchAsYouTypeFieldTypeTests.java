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
import org.elasticsearch.index.mapper.MappedField;
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
            SEARCHABLE,
            null,
            Lucene.STANDARD_ANALYZER,
            Lucene.STANDARD_ANALYZER,
            Collections.emptyMap()
        );
        fieldType.setPrefixField(
            PrefixFieldType.newMappedField(
                NAME,
                new PrefixFieldType(NAME, TextSearchInfo.SIMPLE_MATCH_ONLY, Defaults.MIN_GRAM, Defaults.MAX_GRAM)
            )
        );
        fieldType.setShingleFields(new MappedField[] { new MappedField(NAME, new ShingleFieldType(2, TextSearchInfo.SIMPLE_MATCH_ONLY)) });
        return fieldType;
    }

    public void testTermQuery() {
        final MappedFieldType fieldType = createFieldType();

        assertThat(fieldType.termQuery(NAME, "foo", null), equalTo(new TermQuery(new Term(NAME, "foo"))));

        SearchAsYouTypeFieldType unsearchable = new SearchAsYouTypeFieldType(
            UNSEARCHABLE,
            null,
            Lucene.STANDARD_ANALYZER,
            Lucene.STANDARD_ANALYZER,
            Collections.emptyMap()
        );
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery(NAME, "foo", null));
        assertThat(e.getMessage(), equalTo("Cannot search on field [" + NAME + "] since it is not indexed."));
    }

    public void testTermsQuery() {
        final MappedFieldType fieldType = createFieldType();

        assertThat(
            fieldType.termsQuery(NAME, asList("foo", "bar"), null),
            equalTo(new TermInSetQuery(NAME, asList(new BytesRef("foo"), new BytesRef("bar"))))
        );

        SearchAsYouTypeFieldType unsearchable = new SearchAsYouTypeFieldType(
            UNSEARCHABLE,
            null,
            Lucene.STANDARD_ANALYZER,
            Lucene.STANDARD_ANALYZER,
            Collections.emptyMap()
        );
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.termsQuery(NAME, asList("foo", "bar"), null)
        );
        assertThat(e.getMessage(), equalTo("Cannot search on field [" + NAME + "] since it is not indexed."));
    }

    public void testPrefixQuery() {
        final SearchAsYouTypeFieldType fieldType = createFieldType();

        // this term should be a length that can be rewriteable to a term query on the prefix field
        final String withinBoundsTerm = "foo";
        assertThat(
            fieldType.prefixQuery(NAME, withinBoundsTerm, CONSTANT_SCORE_REWRITE, randomMockContext()),
            equalTo(new ConstantScoreQuery(new TermQuery(new Term(NAME + "._index_prefix", withinBoundsTerm))))
        );

        // our defaults don't allow a situation where a term can be too small

        // this term should be too long to be rewriteable to a term query on the prefix field
        final String longTerm = "toolongforourprefixfieldthistermis";
        assertThat(
            fieldType.prefixQuery(NAME, longTerm, CONSTANT_SCORE_REWRITE, MOCK_CONTEXT),
            equalTo(new PrefixQuery(new Term(NAME, longTerm)))
        );

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> fieldType.prefixQuery(NAME, longTerm, CONSTANT_SCORE_REWRITE, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. "
                + "For optimised prefix queries on text fields please enable [index_prefixes].",
            ee.getMessage()
        );
    }

    public void testFetchSourceValue() throws IOException {
        MappedField mappedField = new MappedField(NAME, createFieldType());

        assertEquals(List.of("value"), fetchSourceValue(mappedField, "value"));
        assertEquals(List.of("42"), fetchSourceValue(mappedField, 42L));
        assertEquals(List.of("true"), fetchSourceValue(mappedField, true));

        MappedField prefixField = PrefixFieldType.newMappedField(
            mappedField.name(),
            new SearchAsYouTypeFieldMapper.PrefixFieldType(mappedField.name(), mappedField.getTextSearchInfo(), 2, 10)
        );
        assertEquals(List.of("value"), fetchSourceValue(prefixField, "value"));
        assertEquals(List.of("42"), fetchSourceValue(prefixField, 42L));
        assertEquals(List.of("true"), fetchSourceValue(prefixField, true));

        MappedField shingleFieldType = new MappedField(
            mappedField.name(),
            new SearchAsYouTypeFieldMapper.ShingleFieldType(5, mappedField.getTextSearchInfo())
        );
        assertEquals(List.of("value"), fetchSourceValue(shingleFieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(shingleFieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(shingleFieldType, true));
    }
}
