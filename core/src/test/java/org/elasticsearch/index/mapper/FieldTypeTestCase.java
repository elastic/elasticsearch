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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.similarity.BM25SimilarityProvider;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Base test case for subclasses of MappedFieldType */
public abstract class FieldTypeTestCase extends ESTestCase {

    /** Abstraction for mutating a property of a MappedFieldType */
    public static abstract class Modifier {
        /** The name of the property that is being modified. Used in test failure messages. */
        public final String property;
        /** true if this modifier only makes types incompatible in strict mode, false otherwise */
        public final boolean strictOnly;
        /** true if reversing the order of checkCompatibility arguments should result in the same conflicts, false otherwise **/
        public final boolean symmetric;

        public Modifier(String property, boolean strictOnly, boolean symmetric) {
            this.property = property;
            this.strictOnly = strictOnly;
            this.symmetric = symmetric;
        }

        /** Modifies the property */
        public abstract void modify(MappedFieldType ft);
        /**
         * Optional method to implement that allows the field type that will be compared to be modified,
         * so that it does not have the default value for the property being modified.
         */
        public void normalizeOther(MappedFieldType other) {}
    }

    private final List<Modifier> modifiers = new ArrayList<>(Arrays.asList(
        new Modifier("boost", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setBoost(1.1f);
            }
        },
        new Modifier("doc_values", false, false) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setHasDocValues(ft.hasDocValues() == false);
            }
        },
        new Modifier("analyzer", false, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setIndexAnalyzer(new NamedAnalyzer("bar", new StandardAnalyzer()));
            }
        },
        new Modifier("analyzer", false, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setIndexAnalyzer(new NamedAnalyzer("bar", new StandardAnalyzer()));
            }
            @Override
            public void normalizeOther(MappedFieldType other) {
                other.setIndexAnalyzer(new NamedAnalyzer("foo", new StandardAnalyzer()));
            }
        },
        new Modifier("search_analyzer", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setSearchAnalyzer(new NamedAnalyzer("bar", new StandardAnalyzer()));
            }
        },
        new Modifier("search_analyzer", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setSearchAnalyzer(new NamedAnalyzer("bar", new StandardAnalyzer()));
            }
            @Override
            public void normalizeOther(MappedFieldType other) {
                other.setSearchAnalyzer(new NamedAnalyzer("foo", new StandardAnalyzer()));
            }
        },
        new Modifier("search_quote_analyzer", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setSearchQuoteAnalyzer(new NamedAnalyzer("bar", new StandardAnalyzer()));
            }
        },
        new Modifier("search_quote_analyzer", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setSearchQuoteAnalyzer(new NamedAnalyzer("bar", new StandardAnalyzer()));
            }
            @Override
            public void normalizeOther(MappedFieldType other) {
                other.setSearchQuoteAnalyzer(new NamedAnalyzer("foo", new StandardAnalyzer()));
            }
        },
        new Modifier("similarity", false, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setSimilarity(new BM25SimilarityProvider("foo", Settings.EMPTY));
            }
        },
        new Modifier("similarity", false, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setSimilarity(new BM25SimilarityProvider("foo", Settings.EMPTY));
            }
            @Override
            public void normalizeOther(MappedFieldType other) {
                other.setSimilarity(new BM25SimilarityProvider("bar", Settings.EMPTY));
            }
        },
        new Modifier("norms.loading", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setNormsLoading(MappedFieldType.Loading.LAZY);
            }
        },
        new Modifier("fielddata", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setFieldDataType(new FieldDataType("foo", Settings.builder().put("loading", "eager").build()));
            }
        },
        new Modifier("null_value", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ft.setNullValue(dummyNullValue);
            }
        }
    ));

    /**
     * Add a mutation that will be tested for all expected semantics of equality and compatibility.
     * These should be added in an @Before method.
     */
    protected void addModifier(Modifier modifier) {
        modifiers.add(modifier);
    }

    private Object dummyNullValue = "dummyvalue";

    /** Sets the null value used by the modifier for null value testing. This should be set in an @Before method. */
    protected void setDummyNullValue(Object value) {
        dummyNullValue = value;
    }

    /** Create a default constructed fieldtype */
    protected abstract MappedFieldType createDefaultFieldType();

    MappedFieldType createNamedDefaultFieldType() {
        MappedFieldType fieldType = createDefaultFieldType();
        fieldType.setNames(new MappedFieldType.Names("foo"));
        return fieldType;
    }

    // TODO: remove this once toString is no longer final on FieldType...
    protected void assertFieldTypeEquals(String property, MappedFieldType ft1, MappedFieldType ft2) {
        if (ft1.equals(ft2) == false) {
            fail("Expected equality, testing property " + property + "\nexpected: " + toString(ft1) + "; \nactual:   " + toString(ft2) + "\n");
        }
    }

    protected void assertFieldTypeNotEquals(String property, MappedFieldType ft1, MappedFieldType ft2) {
        if (ft1.equals(ft2)) {
            fail("Expected inequality, testing property " + property + "\nfirst:  " + toString(ft1) + "; \nsecond: " + toString(ft2) + "\n");
        }
    }

    protected void assertCompatible(String msg, MappedFieldType ft1, MappedFieldType ft2, boolean strict) {
        List<String> conflicts = new ArrayList<>();
        ft1.checkCompatibility(ft2, conflicts, strict);
        assertTrue("Found conflicts for " + msg + ": " + conflicts, conflicts.isEmpty());
    }

    protected void assertNotCompatible(String msg, MappedFieldType ft1, MappedFieldType ft2, boolean strict, String... messages) {
        assert messages.length != 0;
        List<String> conflicts = new ArrayList<>();
        ft1.checkCompatibility(ft2, conflicts, strict);
        for (String message : messages) {
            boolean found = false;
            for (String conflict : conflicts) {
                if (conflict.contains(message)) {
                    found = true;
                }
            }
            assertTrue("Missing conflict for " + msg + ": [" + message + "] in conflicts " + conflicts, found);
        }
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
        MappedFieldType fieldType = createNamedDefaultFieldType();
        MappedFieldType clone = fieldType.clone();
        assertNotSame(clone, fieldType);
        assertEquals(clone.getClass(), fieldType.getClass());
        assertEquals(clone, fieldType);
        assertEquals(clone, clone.clone()); // transitivity

        for (Modifier modifier : modifiers) {
            fieldType = createNamedDefaultFieldType();
            modifier.modify(fieldType);
            clone = fieldType.clone();
            assertNotSame(clone, fieldType);
            assertFieldTypeEquals(modifier.property, clone, fieldType);
        }
    }

    public void testEquals() {
        MappedFieldType ft1 = createNamedDefaultFieldType();
        MappedFieldType ft2 = createNamedDefaultFieldType();
        assertEquals(ft1, ft1); // reflexive
        assertEquals(ft1, ft2); // symmetric
        assertEquals(ft2, ft1);
        assertEquals(ft1.hashCode(), ft2.hashCode());

        for (Modifier modifier : modifiers) {
            ft1 = createNamedDefaultFieldType();
            ft2 = createNamedDefaultFieldType();
            modifier.modify(ft2);
            assertFieldTypeNotEquals(modifier.property, ft1, ft2);
            assertNotEquals("hash code for modified property " + modifier.property, ft1.hashCode(), ft2.hashCode());
            // modify the same property and they are equal again
            modifier.modify(ft1);
            assertFieldTypeEquals(modifier.property, ft1, ft2);
            assertEquals("hash code for modified property " + modifier.property, ft1.hashCode(), ft2.hashCode());
        }
    }

    public void testFreeze() {
        for (Modifier modifier : modifiers) {
            MappedFieldType fieldType = createNamedDefaultFieldType();
            fieldType.freeze();
            try {
                modifier.modify(fieldType);
                fail("expected already frozen exception for property " + modifier.property);
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("already frozen"));
            }
        }
    }

    public void testCheckTypeName() {
        final MappedFieldType fieldType = createNamedDefaultFieldType();
        List<String> conflicts = new ArrayList<>();
        fieldType.checkCompatibility(fieldType, conflicts, random().nextBoolean()); // no exception
        assertTrue(conflicts.toString(), conflicts.isEmpty());

        MappedFieldType bogus = new MappedFieldType() {
            @Override
            public MappedFieldType clone() {return null;}
            @Override
            public String typeName() { return fieldType.typeName();}
        };
        try {
            fieldType.checkCompatibility(bogus, conflicts, random().nextBoolean());
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
        try {
            fieldType.checkCompatibility(other, conflicts, random().nextBoolean());
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("cannot be changed from type"));
        }
        assertTrue(conflicts.toString(), conflicts.isEmpty());
    }

    public void testCheckCompatibility() {
        MappedFieldType ft1 = createNamedDefaultFieldType();
        MappedFieldType ft2 = createNamedDefaultFieldType();
        assertCompatible("default", ft1, ft2, true);
        assertCompatible("default", ft1, ft2, false);
        assertCompatible("default", ft2, ft1, true);
        assertCompatible("default", ft2, ft1, false);

        for (Modifier modifier : modifiers) {
            ft1 = createNamedDefaultFieldType();
            ft2 = createNamedDefaultFieldType();
            modifier.normalizeOther(ft1);
            modifier.modify(ft2);
            if (modifier.strictOnly) {
                String[] conflicts = {
                    "mapper [foo] is used by multiple types",
                    "update [" + modifier.property + "]"
                };
                assertCompatible(modifier.property, ft1, ft2, false);
                assertNotCompatible(modifier.property, ft1, ft2, true, conflicts);
                assertCompatible(modifier.property, ft2, ft1, false); // always symmetric when not strict
                if (modifier.symmetric) {
                    assertNotCompatible(modifier.property, ft2, ft1, true, conflicts);
                } else {
                    assertCompatible(modifier.property, ft2, ft1, true);
                }
            } else {
                // not compatible whether strict or not
                String conflict = "different [" + modifier.property + "]";
                assertNotCompatible(modifier.property, ft1, ft2, true, conflict);
                assertNotCompatible(modifier.property, ft1, ft2, false, conflict);
                if (modifier.symmetric) {
                    assertNotCompatible(modifier.property, ft2, ft1, true, conflict);
                    assertNotCompatible(modifier.property, ft2, ft1, false, conflict);
                } else {
                    assertCompatible(modifier.property, ft2, ft1, true);
                    assertCompatible(modifier.property, ft2, ft1, false);
                }
            }
        }
    }
}
