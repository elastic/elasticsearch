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

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Base test case for subclasses of MappedFieldType */
public abstract class FieldTypeTestCase extends ESTestCase {

    public static final QueryShardContext MOCK_QSC = createMockQueryShardContext(true);
    public static final QueryShardContext MOCK_QSC_DISALLOW_EXPENSIVE = createMockQueryShardContext(false);

    /** Create a default constructed fieldtype */
    protected abstract MappedFieldType createDefaultFieldType();

    MappedFieldType createNamedDefaultFieldType() {
        MappedFieldType fieldType = createDefaultFieldType();
        fieldType.setName("foo");
        return fieldType;
    }

    // TODO: remove this once toString is no longer final on FieldType...
    protected void assertFieldTypeEquals(String property, MappedFieldType ft1, MappedFieldType ft2) {
        if (ft1.equals(ft2) == false) {
            fail("Expected equality, testing property " + property + "\nexpected: " + toString(ft1) + "; \nactual:   " + toString(ft2)
                + "\n");
        }
    }

    protected void assertFieldTypeNotEquals(String property, MappedFieldType ft1, MappedFieldType ft2) {
        if (ft1.equals(ft2)) {
            fail("Expected inequality, testing property " + property + "\nfirst:  " + toString(ft1) + "; \nsecond: " + toString(ft2)
                + "\n");
        }
    }

    protected String toString(MappedFieldType ft) {
        return "MappedFieldType{" +
            "name=" + ft.name() +
            ", boost=" + ft.boost() +
            ", docValues=" + ft.hasDocValues() +
            ", indexAnalyzer=" + ft.indexAnalyzer() +
            ", searchAnalyzer=" + ft.searchAnalyzer() +
            ", searchQuoteAnalyzer=" + ft.searchQuoteAnalyzer() +
            ", similarity=" + ft.similarity() +
            ", eagerGlobalOrdinals=" + ft.eagerGlobalOrdinals() +
            ", nullValue=" + ft.nullValue() +
            ", nullValueAsString='" + ft.nullValueAsString() + "'" +
            "} " + super.toString();
    }

    protected QueryShardContext randomMockShardContext() {
        return randomFrom(MOCK_QSC, MOCK_QSC_DISALLOW_EXPENSIVE);
    }

    static QueryShardContext createMockQueryShardContext(boolean allowExpensiveQueries) {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.allowExpensiveQueries()).thenReturn(allowExpensiveQueries);
        return queryShardContext;
    }

    public void testClone() {
        MappedFieldType fieldType = createNamedDefaultFieldType();
        MappedFieldType clone = fieldType.clone();
        assertNotSame(clone, fieldType);
        assertEquals(clone.getClass(), fieldType.getClass());
        assertEquals(clone, fieldType);
        assertEquals(clone, clone.clone()); // transitivity
    }

    public void testEquals() {
        MappedFieldType ft1 = createNamedDefaultFieldType();
        MappedFieldType ft2 = createNamedDefaultFieldType();
        assertEquals(ft1, ft1); // reflexive
        assertEquals(ft1, ft2); // symmetric
        assertEquals(ft2, ft1);
        assertEquals(ft1.hashCode(), ft2.hashCode());
    }

}
