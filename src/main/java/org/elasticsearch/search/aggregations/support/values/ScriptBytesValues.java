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
package org.elasticsearch.search.aggregations.support.values;

import com.google.common.collect.Iterators;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.support.ScriptValues;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

/**
 *
 */
public class ScriptBytesValues extends BytesValues implements ScriptValues {

    private final BytesRef scratch = new BytesRef();
    final SearchScript script;

    private Iterator<?> iter;
    private Object value;

    public ScriptBytesValues(SearchScript script) {
        super(true); // assume multi-valued
        this.script = script;
    }

    @Override
    public SearchScript script() {
        return script;
    }

    @Override
    public int setDocument(int docId) {
        script.setNextDocId(docId);
        value = script.run();

        if (value == null) {
            iter = Iterators.emptyIterator();
            return 0;
        }

        if (value.getClass().isArray()) {
            final int length = Array.getLength(value);
            // don't use Arrays.asList because the array may be an array of primitives?
            iter = new Iterator<Object>() {

                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < length;
                }

                @Override
                public Object next() {
                    return Array.get(value, i++);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

            };
            return length;
        }

        if (value instanceof Collection) {
            final Collection<?> coll = (Collection<?>) value;
            iter = coll.iterator();
            return coll.size();
        }

        iter = Iterators.singletonIterator(value);
        return 1;
    }

    @Override
    public BytesRef nextValue() {
        final String next = iter.next().toString();
        scratch.copyChars(next);
        return scratch;
    }

}
