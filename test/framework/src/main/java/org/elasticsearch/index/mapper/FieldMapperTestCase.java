/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.search.similarities.BM25Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;

public abstract class FieldMapperTestCase<T extends FieldMapper.Builder<?>> extends ESSingleNodeTestCase {

    protected final Settings SETTINGS = Settings.builder()
        .put("index.version.created", Version.CURRENT)
        .build();

    private final class Modifier {
        final String property;
        final boolean updateable;
        final BiConsumer<T, T> modifier;

        Modifier(String property, boolean updateable, BiConsumer<T, T> modifier) {
            this.property = property;
            this.updateable = updateable;
            this.modifier = modifier;
        }

        void apply(T first, T second) {
            modifier.accept(first, second);
        }
    }

    private Modifier booleanModifier(String name, boolean updateable, BiConsumer<T, Boolean> method) {
        return new Modifier(name, updateable, (a, b) -> {
            method.accept(a, true);
            method.accept(b, false);
        });
    }

    private Object dummyNullValue = "dummyvalue";

    /** Sets the null value used by the modifier for null value testing. This should be set in an @Before method. */
    protected void setDummyNullValue(Object value) {
        dummyNullValue = value;
    }

    protected boolean supportsDocValues() {
        return true;
    }

    protected boolean supportsStore() {
        return true;
    }

    private final List<Modifier> modifiers = new ArrayList<>(Arrays.asList(
        new Modifier("analyzer", false, (a, b) -> {
            a.indexAnalyzer(new NamedAnalyzer("a", AnalyzerScope.INDEX, new StandardAnalyzer()));
            a.indexAnalyzer(new NamedAnalyzer("b", AnalyzerScope.INDEX, new StandardAnalyzer()));
        }),
        new Modifier("boost", true, (a, b) -> {
           a.fieldType().setBoost(1.1f);
           b.fieldType().setBoost(1.2f);
        }),
        new Modifier("doc_values", supportsDocValues() == false, (a, b) -> {
            if (supportsDocValues()) {
                a.docValues(true);
                b.docValues(false);
            }
        }),
        booleanModifier("eager_global_ordinals", true, (a, t) -> a.fieldType().setEagerGlobalOrdinals(t)),
        booleanModifier("norms", false, FieldMapper.Builder::omitNorms),
        new Modifier("null_value", true, (a, b) -> {
            a.fieldType().setNullValue(dummyNullValue);
        }),
        new Modifier("search_analyzer", true, (a, b) -> {
            a.searchAnalyzer(new NamedAnalyzer("a", AnalyzerScope.INDEX, new StandardAnalyzer()));
            a.searchAnalyzer(new NamedAnalyzer("b", AnalyzerScope.INDEX, new StandardAnalyzer()));
        }),
        new Modifier("search_quote_analyzer", true, (a, b) -> {
            a.searchQuoteAnalyzer(new NamedAnalyzer("a", AnalyzerScope.INDEX, new StandardAnalyzer()));
            a.searchQuoteAnalyzer(new NamedAnalyzer("b", AnalyzerScope.INDEX, new StandardAnalyzer()));
        }),
        new Modifier("similarity", false, (a, b) -> {
            a.similarity(new SimilarityProvider("a", new BM25Similarity()));
            b.similarity(new SimilarityProvider("b", new BM25Similarity()));
        }),
        new Modifier("store", supportsStore() == false, (a, b) -> {
            if (supportsStore()) {
                a.store(true);
                b.store(false);
            }
        }),
        new Modifier("term_vector", false, (a, b) -> {
            a.storeTermVectors(true);
            b.storeTermVectors(false);
        }),
        new Modifier("term_vector_positions", false, (a, b) -> {
            a.storeTermVectors(true);
            b.storeTermVectors(true);
            a.storeTermVectorPositions(true);
            b.storeTermVectorPositions(false);
        }),
        new Modifier("term_vector_payloads", false, (a, b) -> {
            a.storeTermVectors(true);
            b.storeTermVectors(true);
            a.storeTermVectorPositions(true);
            b.storeTermVectorPositions(true);
            a.storeTermVectorPayloads(true);
            b.storeTermVectorPayloads(false);
        }),
        new Modifier("term_vector_offsets", false, (a, b) -> {
            a.storeTermVectors(true);
            b.storeTermVectors(true);
            a.storeTermVectorPositions(true);
            b.storeTermVectorPositions(true);
            a.storeTermVectorOffsets(true);
            b.storeTermVectorOffsets(false);
        })
    ));

    /**
     * Add type-specific modifiers for consistency checking.
     *
     * This should be called in a {@code @Before} method
     */
    protected void addModifier(String property, boolean updateable, BiConsumer<T, T> method) {
        modifiers.add(new Modifier(property, updateable, method));
    }

    /**
     * Add type-specific modifiers for consistency checking.
     *
     * This should be called in a {@code @Before} method
     */
    protected void addBooleanModifier(String property, boolean updateable, BiConsumer<T, Boolean> method) {
        modifiers.add(new Modifier(property, updateable, (a, b) -> {
            method.accept(a, true);
            method.accept(b, false);
        }));
    }

    protected abstract T newBuilder();

    public void testMergeConflicts() {
        Mapper.BuilderContext context = new Mapper.BuilderContext(SETTINGS, new ContentPath(1));
        T builder1 = newBuilder();
        T builder2 = newBuilder();
        {
            FieldMapper mapper = (FieldMapper) builder1.build(context);
            FieldMapper toMerge = (FieldMapper) builder2.build(context);
            mapper.merge(toMerge);  // identical mappers should merge with no issue
        }
        {
            FieldMapper mapper = (FieldMapper) newBuilder().build(context);
            FieldMapper toMerge = new MockFieldMapper("bogus") {
                @Override
                protected String contentType() {
                    return "bogustype";
                }
            };
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> mapper.merge(toMerge));
            assertThat(e.getMessage(), containsString("cannot be changed from type"));
            assertThat(e.getMessage(), containsString("bogustype"));
        }
        for (Modifier modifier : modifiers) {
            builder1 = newBuilder();
            builder2 = newBuilder();
            modifier.apply(builder1, builder2);
            FieldMapper mapper = (FieldMapper) builder1.build(context);
            FieldMapper toMerge = (FieldMapper) builder2.build(context);
            if (modifier.updateable) {
                mapper.merge(toMerge);
            } else {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    "Expected an error when merging property difference " + modifier.property, () -> mapper.merge(toMerge));
                assertThat(e.getMessage(), containsString(modifier.property));
            }
        }
    }

}
