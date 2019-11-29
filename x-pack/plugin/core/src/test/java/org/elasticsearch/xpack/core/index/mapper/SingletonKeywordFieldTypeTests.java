/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.index.mapper;

import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.core.index.mapper.SingletonKeywordFieldMapper.SingletonKeywordFieldType;
import org.junit.Before;

public class SingletonKeywordFieldTypeTests extends FieldTypeTestCase {

    @Before
    public void setupProperties() {
        addModifier(new Modifier("value", false) {
            @Override
            public void modify(MappedFieldType type) {
                ((SingletonKeywordFieldType) type).setValue("bar");
            }
        });
    }

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new SingletonKeywordFieldType();
    }

    public void testTermQuery() {
        SingletonKeywordFieldType ft = new SingletonKeywordFieldType();
        ft.setValue("foo");
        assertEquals(new MatchAllDocsQuery(), ft.termQuery("foo", null));
        assertEquals(new MatchNoDocsQuery(), ft.termQuery("bar", null));
    }

    public void testTermsQuery() {
        SingletonKeywordFieldType ft = new SingletonKeywordFieldType();
        ft.setValue("foo");
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery(Collections.singletonList("foo"), null));
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery(Arrays.asList("bar", "foo", "quux"), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Collections.emptyList(), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Collections.singletonList("bar"), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Arrays.asList("bar", "quux"), null));
    }

    public void testWildcardQuery() {
        SingletonKeywordFieldType ft = new SingletonKeywordFieldType();
        ft.setValue("foo");
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("f*o", null, null));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("b*r", null, null));
    }

    public void testPrefixQuery() {
        SingletonKeywordFieldType ft = new SingletonKeywordFieldType();
        ft.setValue("foo");
        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("fo", null, null));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("ba", null, null));
    }
}
