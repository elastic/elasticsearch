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

import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.similarity.BM25SimilarityProvider;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

/** Base test case for subclasses of MappedFieldType */
public abstract class FieldTypeTestCase extends ESTestCase {

    /** Create a default constructed fieldtype */
    protected abstract MappedFieldType createDefaultFieldType();

    MappedFieldType createNamedDefaultFieldType(String name) {
        MappedFieldType fieldType = createDefaultFieldType();
        fieldType.setNames(new MappedFieldType.Names(name));
        return fieldType;
    }

    /** A dummy null value to use when modifying null value */
    protected Object dummyNullValue() {
        return "dummyvalue";
    }

    /** Returns the number of properties that can be modified for the fieldtype */
    protected int numProperties() {
        return 10;
    }

    /** Modifies a property, identified by propNum, on the given fieldtype */
    protected void modifyProperty(MappedFieldType ft, int propNum) {
        switch (propNum) {
            case 0: ft.setNames(new MappedFieldType.Names("dummy")); break;
            case 1: ft.setBoost(1.1f); break;
            case 2: ft.setHasDocValues(!ft.hasDocValues()); break;
            case 3: ft.setIndexAnalyzer(Lucene.STANDARD_ANALYZER); break;
            case 4: ft.setSearchAnalyzer(Lucene.STANDARD_ANALYZER); break;
            case 5: ft.setSearchQuoteAnalyzer(Lucene.STANDARD_ANALYZER); break;
            case 6: ft.setSimilarity(new BM25SimilarityProvider("foo", Settings.EMPTY)); break;
            case 7: ft.setNormsLoading(MappedFieldType.Loading.LAZY); break;
            case 8: ft.setFieldDataType(new FieldDataType("foo", Settings.builder().put("loading", "eager").build())); break;
            case 9: ft.setNullValue(dummyNullValue()); break;
            default: fail("unknown fieldtype property number " + propNum);
        }
    }

    // TODO: remove this once toString is no longer final on FieldType...
    protected void assertEquals(int i, MappedFieldType ft1, MappedFieldType ft2) {
        assertEquals("prop " + i + "\nexpected: " + toString(ft1) + "; \nactual:   " + toString(ft2), ft1, ft2);
    }

    protected String toString(MappedFieldType ft) {
        return "MappedFieldType{" +
            "names=" + ft.names() +
            ", boost=" + ft.boost() +
            ", docValues=" + ft.hasDocValues() +
            ", indexAnalyzer=" + ft.indexAnalyzer() +
            ", searchAnalyzer=" + ft.searchAnalyzer() +
            ", searchQuoteAnalyzer=" + ft.searchQuoteAnalyzer() +
            ", similarity=" + ft.similarity() +
            ", normsLoading=" + ft.normsLoading() +
            ", fieldDataType=" + ft.fieldDataType() +
            ", nullValue=" + ft.nullValue() +
            ", nullValueAsString='" + ft.nullValueAsString() + "'" +
            "} " + super.toString();
    }

    public void testClone() {
        MappedFieldType fieldType = createNamedDefaultFieldType("foo");
        MappedFieldType clone = fieldType.clone();
        assertNotSame(clone, fieldType);
        assertEquals(clone.getClass(), fieldType.getClass());
        assertEquals(clone, fieldType);
        assertEquals(clone, clone.clone()); // transitivity

        for (int i = 0; i < numProperties(); ++i) {
            fieldType = createNamedDefaultFieldType("foo");
            modifyProperty(fieldType, i);
            clone = fieldType.clone();
            assertNotSame(clone, fieldType);
            assertEquals(i, clone, fieldType);
        }
    }

    public void testEquals() {
        MappedFieldType ft1 = createNamedDefaultFieldType("foo");
        MappedFieldType ft2 = createNamedDefaultFieldType("foo");
        assertEquals(ft1, ft1); // reflexive
        assertEquals(ft1, ft2); // symmetric
        assertEquals(ft2, ft1);
        assertEquals(ft1.hashCode(), ft2.hashCode());

        for (int i = 0; i < numProperties(); ++i) {
            ft2 = createNamedDefaultFieldType("foo");
            modifyProperty(ft2, i);
            assertNotEquals(ft1, ft2);
            assertNotEquals(ft1.hashCode(), ft2.hashCode());
        }
    }

    public void testFreeze() {
        for (int i = 0; i < numProperties(); ++i) {
            MappedFieldType fieldType = createNamedDefaultFieldType("foo");
            fieldType.freeze();
            try {
                modifyProperty(fieldType, i);
                fail("expected already frozen exception for property " + i);
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("already frozen"));
            }
        }
    }

    public void testCheckTypeName() {
        final MappedFieldType fieldType = createNamedDefaultFieldType("foo");
        List<String> conflicts = new ArrayList<>();
        fieldType.checkTypeName(fieldType, conflicts);
        assertTrue(conflicts.toString(), conflicts.isEmpty());

        MappedFieldType bogus = new MappedFieldType() {
            @Override
            public MappedFieldType clone() {return null;}
            @Override
            public String typeName() { return fieldType.typeName();}
        };
        try {
            fieldType.checkTypeName(bogus, conflicts);
            fail("expected bad types exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("Type names equal"));
        }
        assertTrue(conflicts.toString(), conflicts.isEmpty());

        MappedFieldType other = new MappedFieldType() {
            @Override
            public MappedFieldType clone() {return null;}
            @Override
            public String typeName() { return "othertype";}
        };
        fieldType.checkTypeName(other, conflicts);
        assertFalse(conflicts.isEmpty());
        assertTrue(conflicts.get(0).contains("cannot be changed from type"));
        assertEquals(1, conflicts.size());
    }
}
