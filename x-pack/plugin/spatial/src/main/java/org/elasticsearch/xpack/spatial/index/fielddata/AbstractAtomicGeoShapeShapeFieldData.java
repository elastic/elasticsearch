/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.util.Collection;
import java.util.Collections;

public abstract class AbstractAtomicGeoShapeShapeFieldData implements LeafGeoShapeFieldData {

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("scripts and term aggs are not supported by geo_shape doc values");
    }

    @Override
    public final ScriptDocValues.BytesRefs getScriptValues() {
        throw new UnsupportedOperationException("scripts are not supported by geo_shape doc values");
    }

    public static LeafGeoShapeFieldData empty(final int maxDoc) {
        return new AbstractAtomicGeoShapeShapeFieldData() {

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

            @Override
            public void close() {
            }

            @Override
            public MultiGeoShapeValues getGeoShapeValues() {
                return MultiGeoShapeValues.EMPTY;
            }
        };
    }
}
