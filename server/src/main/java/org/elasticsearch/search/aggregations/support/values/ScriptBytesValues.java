/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.support.values;

import org.apache.lucene.search.Scorable;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.script.AggregationScript;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;

/**
 * {@link SortedBinaryDocValues} implementation that reads values from a script.
 */
public class ScriptBytesValues extends SortingBinaryDocValues implements ScorerAware {

    private final AggregationScript script;

    public ScriptBytesValues(AggregationScript script) {
        super();
        this.script = script;
    }

    private void set(int i, Object o) {
        if (o == null) {
            values[i].clear();
        } else {
            CollectionUtils.ensureNoSelfReferences(o, "ScriptBytesValues value");
            values[i].copyChars(o.toString());
        }
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        script.setDocument(doc);
        final Object value = script.execute();
        if (value == null) {
            return false;
        } else if (value.getClass().isArray()) {
            count = Array.getLength(value);
            if (count == 0) {
                return false;
            }
            grow();
            for (int i = 0; i < count; ++i) {
                set(i, Array.get(value, i));
            }
        } else if (value instanceof final Collection<?> coll) {
            count = coll.size();
            if (count == 0) {
                return false;
            }
            grow();
            int i = 0;
            for (Object v : coll) {
                set(i++, v);
            }
        } else {
            count = 1;
            set(0, value);
        }
        sort();
        return true;
    }

    @Override
    public void setScorer(Scorable scorer) {
        script.setScorer(scorer);
    }
}
