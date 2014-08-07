package org.apache.lucene.expressions;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.SortField;

/**
 * Simple class that binds expression variable names to {@link SortField}s
 * or other {@link Expression}s.
 * <p>
 * Example usage:
 * <pre class="prettyprint">
 *   XSimpleBindings bindings = new XSimpleBindings();
 *   // document's text relevance score
 *   bindings.add(new SortField("_score", SortField.Type.SCORE));
 *   // integer NumericDocValues field (or from FieldCache) 
 *   bindings.add(new SortField("popularity", SortField.Type.INT));
 *   // another expression
 *   bindings.add("recency", myRecencyExpression);
 *
 *   // create a sort field in reverse order
 *   Sort sort = new Sort(expr.getSortField(bindings, true));
 * </pre>
 *
 * @lucene.experimental
 */
public final class XSimpleBindings extends Bindings {

    static {
        assert org.elasticsearch.Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_4_9: "Remove this code once we upgrade to Lucene 4.10 (LUCENE-5806)";
    }

    final Map<String,Object> map = new HashMap<>();

    /** Creates a new empty Bindings */
    public XSimpleBindings() {}

    /**
     * Adds a SortField to the bindings.
     * <p>
     * This can be used to reference a DocValuesField, a field from
     * FieldCache, the document's score, etc. 
     */
    public void add(SortField sortField) {
        map.put(sortField.getField(), sortField);
    }

    /**
     * Bind a {@link ValueSource} directly to the given name.
     */
    public void add(String name, ValueSource source) { map.put(name, source); }

    /**
     * Adds an Expression to the bindings.
     * <p>
     * This can be used to reference expressions from other expressions. 
     */
    public void add(String name, Expression expression) {
        map.put(name, expression);
    }

    @Override
    public ValueSource getValueSource(String name) {
        Object o = map.get(name);
        if (o == null) {
            throw new IllegalArgumentException("Invalid reference '" + name + "'");
        } else if (o instanceof Expression) {
            return ((Expression)o).getValueSource(this);
        } else if (o instanceof ValueSource) {
            return ((ValueSource)o);
        }
        SortField field = (SortField) o;
        switch(field.getType()) {
            case INT:
                return new IntFieldSource(field.getField());
            case LONG:
                return new LongFieldSource(field.getField());
            case FLOAT:
                return new FloatFieldSource(field.getField());
            case DOUBLE:
                return new DoubleFieldSource(field.getField());
            case SCORE:
                return getScoreValueSource();
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Traverses the graph of bindings, checking there are no cycles or missing references 
     * @throws IllegalArgumentException if the bindings is inconsistent 
     */
    public void validate() {
        for (Object o : map.values()) {
            if (o instanceof Expression) {
                Expression expr = (Expression) o;
                try {
                    expr.getValueSource(this);
                } catch (StackOverflowError e) {
                    throw new IllegalArgumentException("Recursion Error: Cycle detected originating in (" + expr.sourceText + ")");
                }
            }
        }
    }
}
