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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.Defaults;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.PrefixFieldType;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.ShingleFieldType;
import org.junit.Before;

import static java.util.Arrays.asList;
import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_REWRITE;
import static org.hamcrest.Matchers.equalTo;

public class ShingleFieldTypeTests extends FieldTypeTestCase {

    private static final String NAME = "a_field";
    private static final String PREFIX_NAME = NAME + "._index_prefix";

    @Override
    protected MappedFieldType createDefaultFieldType() {
        final ShingleFieldType shingleFieldType = new ShingleFieldType(Defaults.FIELD_TYPE, 1);
        shingleFieldType.setName(NAME);
        shingleFieldType.setPrefixFieldType(new PrefixFieldType(NAME, PREFIX_NAME, Defaults.MIN_GRAM, Defaults.MAX_GRAM));
        return shingleFieldType;
    }

    public void testTermQuery() {
        final MappedFieldType fieldType = createDefaultFieldType();

        fieldType.setIndexOptions(IndexOptions.DOCS);
        assertThat(fieldType.termQuery("foo", null), equalTo(new TermQuery(new Term(NAME, "foo"))));

        fieldType.setIndexOptions(IndexOptions.NONE);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fieldType.termQuery("foo", null));
        assertThat(e.getMessage(), equalTo("Cannot search on field [" + NAME + "] since it is not indexed."));
    }

    public void testTermsQuery() {
        final MappedFieldType fieldType = createDefaultFieldType();

        fieldType.setIndexOptions(IndexOptions.DOCS);
        assertThat(fieldType.termsQuery(asList("foo", "bar"), null),
            equalTo(new TermInSetQuery(NAME, asList(new BytesRef("foo"), new BytesRef("bar")))));

        fieldType.setIndexOptions(IndexOptions.NONE);
        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> fieldType.termsQuery(asList("foo", "bar"), null));
        assertThat(e.getMessage(), equalTo("Cannot search on field [" + NAME + "] since it is not indexed."));
    }

    public void testPrefixQuery() {
        ShingleFieldType fieldType = (ShingleFieldType) createDefaultFieldType();

        // this term should be a length that can be rewriteable to a term query on the prefix field
        final String withinBoundsTerm = "foo";
        assertThat(fieldType.prefixQuery(withinBoundsTerm, CONSTANT_SCORE_REWRITE, null),
            equalTo(new ConstantScoreQuery(new TermQuery(new Term(PREFIX_NAME, withinBoundsTerm)))));

        // our defaults don't allow a situation where a term can be too small

        // this term should be too long to be rewriteable to a term query on the prefix field
        final String longTerm = "toolongforourprefixfieldthistermis";
        assertThat(fieldType.prefixQuery(longTerm, CONSTANT_SCORE_REWRITE, null),
            equalTo(new PrefixQuery(new Term(NAME, longTerm))));
    }
}
