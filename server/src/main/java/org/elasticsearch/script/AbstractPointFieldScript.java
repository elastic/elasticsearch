/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.ToXContentFragment;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Script producing points and geo points. Similarly to what {@link LatLonDocValuesField} does,
 * it encodes the points as a long value. Must be extended for the specific type of point.
 */
public abstract class AbstractPointFieldScript<T extends ToXContentFragment> extends AbstractLongFieldScript {

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    protected final PointFieldScriptEncoder<T> pointFieldScriptEncoder;

    public interface LeafFactory<T extends ToXContentFragment> {
        AbstractPointFieldScript<T> newInstance(LeafReaderContext ctx);
    }

    public AbstractPointFieldScript(
        String fieldName,
        Map<String, Object> params,
        SearchLookup searchLookup,
        LeafReaderContext ctx,
        PointFieldScriptEncoder<T> pointFieldScriptEncoder
    ) {
        super(fieldName, params, searchLookup, ctx);
        this.pointFieldScriptEncoder = pointFieldScriptEncoder;
    }

    protected interface PointFieldScriptEncoder<T extends ToXContentFragment> {
        void consumeValues(long[] values, int count, Consumer<T> consumer);
    }

    /**
     * Consumers must copy the emitted Point(s) if stored.
     */
    public void runGeoPointForDoc(int doc, Consumer<T> consumer) {
        runForDoc(doc);
        pointFieldScriptEncoder.consumeValues(values(), count(), consumer);
    }

    @Override
    protected List<Object> extractFromSource(String path) {
        Object value = XContentMapValues.extractValue(path, sourceLookup.source());
        if (value instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            if (list.size() > 0 && list.get(0) instanceof Number) {
                // [2, 1]: two values but one single point, return it as a list or each value will be seen as a different geopoint.
                return Collections.singletonList(list);
            }
            // e.g. [ [2,1], {lat:2, lon:1} ]
            return list;
        }
        // e.g. {lat: 2, lon: 1}
        return Collections.singletonList(value);
    }

    @Override
    protected void emitFromObject(Object value) {
        if (value instanceof List<?> values) {
            if (values.size() > 0 && values.get(0) instanceof Number) {
                emitPoint(value);
            } else {
                for (Object point : values) {
                    emitPoint(point);
                }
            }
        } else {
            emitPoint(value);
        }
    }

    protected abstract void emitPoint(Object point);

    protected abstract void emit(double a, double b);
}
