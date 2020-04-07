/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.entities.mapper;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;

public class TestEntityKeywordFieldType extends FieldTypeTestCase {

    // ESTC, Elastic.co -> elastic
    private static SynonymMap buildSynonyms() throws IOException {
        SynonymMap.Builder builder = new SynonymMap.Builder();
        builder.add(new CharsRef("ESTC"), new CharsRef("elastic"), true);
        builder.add(new CharsRef("Elastic.co"), new CharsRef("elastic"), true);
        return builder.build();
    }

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new EntityKeywordFieldMapper.EntityKeywordFieldType();
    }

    public void testTermExpansion() throws IOException {
        EntityKeywordFieldMapper.EntityKeywordFieldType ft = new EntityKeywordFieldMapper.EntityKeywordFieldType();
        ft.setSynonymMap(buildSynonyms());
        ft.setName("field");
        ft.setSearchAnalyzer(new NamedAnalyzer("entity", AnalyzerScope.INDEX, new KeywordAnalyzer()));
        Query q = ft.termQuery("ESTC", randomMockShardContext());
        Query expected = new SynonymQuery.Builder("field")
            .addTerm(new Term("field", "elastic"))
            .addTerm(new Term("field", "Elastic.co"))
            .addTerm(new Term("field", "ESTC"))
            .build();
        assertEquals(expected, q);
    }
}
