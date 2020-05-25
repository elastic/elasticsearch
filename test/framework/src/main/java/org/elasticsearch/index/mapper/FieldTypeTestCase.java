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
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Base test case for subclasses of MappedFieldType */
public abstract class FieldTypeTestCase<T extends MappedFieldType> extends ESTestCase {

    public static final QueryShardContext MOCK_QSC = createMockQueryShardContext(true);
    public static final QueryShardContext MOCK_QSC_DISALLOW_EXPENSIVE = createMockQueryShardContext(false);

    /** Create a default constructed fieldtype */
    protected abstract T createDefaultFieldType();

    T createNamedDefaultFieldType() {
        T fieldType = createDefaultFieldType();
        fieldType.setName("foo");
        return fieldType;
    }

    @SuppressWarnings("unchecked")
    private final List<EqualsHashCodeTestUtils.MutateFunction<T>> modifiers = new ArrayList<>(List.of(
        t -> {
            MappedFieldType copy = t.clone();
            copy.setName(t.name() + "-mutated");
            return (T) copy;
        },
        t -> {
            MappedFieldType copy = t.clone();
            copy.setBoost(t.boost() + 1);
            return (T) copy;
        },
        t -> {
            MappedFieldType copy = t.clone();
            NamedAnalyzer a = t.searchAnalyzer();
            if (a == null) {
                copy.setSearchAnalyzer(new NamedAnalyzer("mutated", AnalyzerScope.INDEX, new StandardAnalyzer()));
                return (T) copy;
            }
            copy.setSearchAnalyzer(new NamedAnalyzer(a.name() + "-mutated", a.scope(), a.analyzer()));
            return (T) copy;
        },
        t -> {
            MappedFieldType copy = t.clone();
            NamedAnalyzer a = t.searchQuoteAnalyzer();
            if (a == null) {
                copy.setSearchQuoteAnalyzer(new NamedAnalyzer("mutated", AnalyzerScope.INDEX, new StandardAnalyzer()));
                return (T) copy;
            }
            copy.setSearchQuoteAnalyzer(new NamedAnalyzer(a.name() + "-mutated", a.scope(), a.analyzer()));
            return (T) copy;
        },
        t -> {
            MappedFieldType copy = t.clone();
            copy.setNullValue(new Object());
            return (T) copy;
        },
        t -> {
            MappedFieldType copy = t.clone();
            copy.setEagerGlobalOrdinals(t.eagerGlobalOrdinals() == false);
            return (T) copy;
        },
        t -> {
            MappedFieldType copy = t.clone();
            Map<String, String> meta = new HashMap<>(t.meta());
            meta.put("bogus", "bogus");
            copy.setMeta(meta);
            return (T) copy;
        }
    ));

    protected void addModifier(EqualsHashCodeTestUtils.MutateFunction<T> modifier) {
        modifiers.add(modifier);
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
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(fieldType, MappedFieldType::clone);
    }

    @SuppressWarnings("unchecked")
    public void testEquals() {
        for (EqualsHashCodeTestUtils.MutateFunction<T> modifier : modifiers) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createNamedDefaultFieldType(),
                t -> (T) t.clone(), modifier);
        }
    }

}
