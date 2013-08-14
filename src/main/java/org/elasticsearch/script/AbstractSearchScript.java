/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.script;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.FieldsLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Map;

/**
 * A base class for any script type that is used during the search process (custom score, facets, and so on).
 * <p/>
 * <p>If the script returns a specific numeric type, consider overriding the type specific base classes
 * such as {@link AbstractDoubleSearchScript}, {@link AbstractFloatSearchScript} and {@link AbstractLongSearchScript}
 * for better performance.
 * <p/>
 * <p>The use is required to implement the {@link #run()} method.
 */
public abstract class AbstractSearchScript extends AbstractExecutableScript implements SearchScript {

    private SearchLookup lookup;

    private float score = Float.NaN;

    /**
     * Returns the current score and only applicable when used as a scoring script in a custom score query!.
     * For other cases, use {@link #doc()} and get the score from it.
     */
    protected final float score() {
        return score;
    }

    /**
     * Returns the doc lookup allowing to access field data (cached) values as well as the current document score
     * (where applicable).
     */
    protected final DocLookup doc() {
        return lookup.doc();
    }

    /**
     * Returns field data strings access for the provided field.
     */
    protected ScriptDocValues.Strings docFieldStrings(String field) {
        return (ScriptDocValues.Strings) doc().get(field);
    }

    /**
     * Returns field data double (floating point) access for the provided field.
     */
    protected ScriptDocValues.Doubles docFieldDoubles(String field) {
        return (ScriptDocValues.Doubles) doc().get(field);
    }

    /**
     * Returns field data long (integers) access for the provided field.
     */
    protected ScriptDocValues.Longs docFieldLongs(String field) {
        return (ScriptDocValues.Longs) doc().get(field);
    }

    /**
     * Allows to access the actual source (loaded and parsed).
     */
    protected final SourceLookup source() {
        return lookup.source();
    }

    /**
     * Allows to access the *stored* fields.
     */
    protected final FieldsLookup fields() {
        return lookup.fields();
    }

    void setLookup(SearchLookup lookup) {
        this.lookup = lookup;
    }

    @Override
    public void setScorer(Scorer scorer) {
        lookup.setScorer(scorer);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        lookup.setNextReader(context);
    }

    @Override
    public void setNextDocId(int doc) {
        lookup.setNextDocId(doc);
    }

    @Override
    public void setNextSource(Map<String, Object> source) {
        lookup.source().setNextSource(source);
    }

    @Override
    public void setNextScore(float score) {
        this.score = score;
    }

    @Override
    public float runAsFloat() {
        return ((Number) run()).floatValue();
    }

    @Override
    public long runAsLong() {
        return ((Number) run()).longValue();
    }

    @Override
    public double runAsDouble() {
        return ((Number) run()).doubleValue();
    }
}