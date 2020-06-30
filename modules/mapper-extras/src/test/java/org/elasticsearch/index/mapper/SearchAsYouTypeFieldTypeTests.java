/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.Defaults;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.PrefixFieldType;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SearchAsYouTypeFieldType;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.ShingleFieldType;

import java.util.Collections;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_REWRITE;
import static org.hamcrest.Matchers.equalTo;

public class SearchAsYouTypeFieldTypeTests extends FieldTypeTestCase<MappedFieldType> {

    private static final String NAME = "a_field";
    private static final FieldType UNSEARCHABLE = new FieldType();
    static {
        UNSEARCHABLE.setIndexOptions(IndexOptions.NONE);
        UNSEARCHABLE.freeze();
    }

    @Override
    protected SearchAsYouTypeFieldType createDefaultFieldType(String name, Map<String, String> meta) {
        final SearchAsYouTypeFieldType fieldType = new SearchAsYouTypeFieldType(name, Defaults.FIELD_TYPE, null, meta);
        fieldType.setPrefixField(new PrefixFieldType(NAME, Defaults.FIELD_TYPE, Defaults.MIN_GRAM, Defaults.MAX_GRAM));
        fieldType.setShingleFields(new ShingleFieldType[] { new ShingleFieldType(fieldType.name(), 2, Defaults.FIELD_TYPE) });
        return fieldType;
    }

    public void testTermQuery() {
        final MappedFieldType fieldType = createDefaultFieldType(NAME, Collections.emptyMap());

        assertThat(fieldType.termQuery("foo", null), equalTo(new TermQuery(new Term(NAME, "foo"))));

        SearchAsYouTypeFieldType unsearchable = new SearchAsYouTypeFieldType(NAME, UNSEARCHABLE, null, Collections.emptyMap());
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("foo", null));
        assertThat(e.getMessage(), equalTo("Cannot search on field [" + NAME + "] since it is not indexed."));
    }

    public void testTermsQuery() {
        final MappedFieldType fieldType = createDefaultFieldType(NAME, Collections.emptyMap());

        assertThat(fieldType.termsQuery(asList("foo", "bar"), null),
            equalTo(new TermInSetQuery(NAME, asList(new BytesRef("foo"), new BytesRef("bar")))));

        SearchAsYouTypeFieldType unsearchable = new SearchAsYouTypeFieldType(NAME, UNSEARCHABLE, null, Collections.emptyMap());
        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> unsearchable.termsQuery(asList("foo", "bar"), null));
        assertThat(e.getMessage(), equalTo("Cannot search on field [" + NAME + "] since it is not indexed."));
    }

    public void testPrefixQuery() {
        final SearchAsYouTypeFieldType fieldType = createDefaultFieldType(NAME, Collections.emptyMap());

        // this term should be a length that can be rewriteable to a term query on the prefix field
        final String withinBoundsTerm = "foo";
        assertThat(fieldType.prefixQuery(withinBoundsTerm, CONSTANT_SCORE_REWRITE, randomMockShardContext()),
            equalTo(new ConstantScoreQuery(new TermQuery(new Term(NAME + "._index_prefix", withinBoundsTerm)))));

        // our defaults don't allow a situation where a term can be too small

        // this term should be too long to be rewriteable to a term query on the prefix field
        final String longTerm = "toolongforourprefixfieldthistermis";
        assertThat(fieldType.prefixQuery(longTerm, CONSTANT_SCORE_REWRITE, MOCK_QSC),
            equalTo(new PrefixQuery(new Term(NAME, longTerm))));

        ElasticsearchException ee = expectThrows(ElasticsearchException.class,
                () -> fieldType.prefixQuery(longTerm, CONSTANT_SCORE_REWRITE, MOCK_QSC_DISALLOW_EXPENSIVE));
        assertEquals("[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. " +
                "For optimised prefix queries on text fields please enable [index_prefixes].", ee.getMessage());
    }
}
