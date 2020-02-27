/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper.ConstantKeywordFieldType;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ConstantKeywordFieldTypeTests extends FieldTypeTestCase {

    @Before
    public void setupProperties() {
        addModifier(new Modifier("value", false) {
            @Override
            public void modify(MappedFieldType type) {
                ((ConstantKeywordFieldType) type).setValue("bar");
            }
        });
    }

    public void testSetValue() {
        ConstantKeywordFieldType ft1 = new ConstantKeywordFieldType();
        ft1.setName("field");
        ConstantKeywordFieldType ft2 = new ConstantKeywordFieldType();
        ft2.setName("field");
        ft2.setValue("bar");
        List<String> conflicts = new ArrayList<>();
        ft1.checkCompatibility(ft2, conflicts);
        assertEquals(Collections.emptyList(), conflicts);
    }

    public void testUnsetValue() {
        ConstantKeywordFieldType ft1 = new ConstantKeywordFieldType();
        ft1.setName("field");
        ft1.setValue("foo");
        ConstantKeywordFieldType ft2 = new ConstantKeywordFieldType();
        ft2.setName("field");
        List<String> conflicts = new ArrayList<>();
        ft1.checkCompatibility(ft2, conflicts);
        assertEquals(Collections.singletonList("mapper [field] cannot unset [value]"), conflicts);
    }

    @Override
    protected MappedFieldType createDefaultFieldType() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType();
        ft.setValue("foo");
        return ft;
    }

    public void testTermQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType();
        assertEquals(new MatchNoDocsQuery(), ft.termQuery("foo", null));
        ft.setValue("foo");
        assertEquals(new MatchAllDocsQuery(), ft.termQuery("foo", null));
        assertEquals(new MatchNoDocsQuery(), ft.termQuery("bar", null));
    }

    public void testTermsQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType();
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Collections.singletonList("foo"), null));
        ft.setValue("foo");
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery(Collections.singletonList("foo"), null));
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery(Arrays.asList("bar", "foo", "quux"), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Collections.emptyList(), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Collections.singletonList("bar"), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Arrays.asList("bar", "quux"), null));
    }

    public void testWildcardQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType();
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("f*o", null, null));
        ft.setValue("foo");
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("f*o", null, null));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("b*r", null, null));
    }

    public void testPrefixQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType();
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("fo", null, null));
        ft.setValue("foo");
        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("fo", null, null));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("ba", null, null));
    }

    public void testRangeQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType();
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery(null, "foo", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("foo", null, randomBoolean(), randomBoolean(), null, null, null, null));
        ft.setValue("foo");
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery("foo", null, true, randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("foo", null, false, randomBoolean(), null, null, null, null));
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery(null, "foo", randomBoolean(), true, null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery(null, "foo", randomBoolean(), false, null, null, null, null));
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery("abc", "xyz", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("abc", "def", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("mno", "xyz", randomBoolean(), randomBoolean(), null, null, null, null));
    }

    public void testFuzzyQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType();
        assertEquals(new MatchNoDocsQuery(), ft.fuzzyQuery("fooquux", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
        ft.setValue("foobar");
        assertEquals(new MatchAllDocsQuery(), ft.fuzzyQuery("foobaz", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
        assertEquals(new MatchNoDocsQuery(), ft.fuzzyQuery("fooquux", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
    }

    public void testRegexpQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType();
        assertEquals(new MatchNoDocsQuery(), ft.regexpQuery("f..o", RegExp.ALL, 10, null, null));
        ft.setValue("foo");
        assertEquals(new MatchAllDocsQuery(), ft.regexpQuery("f.o", RegExp.ALL, 10, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.regexpQuery("f..o", RegExp.ALL, 10, null, null));
    }
}
