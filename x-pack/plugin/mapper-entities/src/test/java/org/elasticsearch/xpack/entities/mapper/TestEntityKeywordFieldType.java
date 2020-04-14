/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.entities.mapper;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

public class TestEntityKeywordFieldType extends FieldTypeTestCase {

    private static final EntityMap entities = new EntityMap.Builder(BigArrays.NON_RECYCLING_INSTANCE)
        .addEntity(new BytesRef("elastic"), new BytesRef("ESTC"), new BytesRef("Elastic.co"))
        .build();

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new EntityKeywordFieldMapper.EntityKeywordFieldType();
    }

    public void testTermExpansion() {
        EntityKeywordFieldMapper.EntityKeywordFieldType ft = new EntityKeywordFieldMapper.EntityKeywordFieldType();
        ft.setEntityMap(entities);
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
