/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.ShapeValues;

public abstract class AbstractAtomicGeoShapeShapeFieldData extends LeafShapeFieldData<GeoPoint> {

    public AbstractAtomicGeoShapeShapeFieldData(ToScriptFieldFactory<ShapeValues<GeoPoint>> toScriptFieldFactory) {
        super(toScriptFieldFactory);
    }

    public static LeafShapeFieldData<GeoPoint> empty(final int maxDoc, ToScriptFieldFactory<ShapeValues<GeoPoint>> toScriptFieldFactory) {
        return new LeafShapeFieldData.Empty<>(toScriptFieldFactory, GeoShapeValues.EMPTY);
    }

    public static final class GeoShapeScriptValues extends LeafShapeFieldData.ShapeScriptValues<GeoPoint> {

        public GeoShapeScriptValues(GeometrySupplier<GeoPoint, ShapeValues.ShapeValue<GeoPoint>> supplier) {
            super(supplier);
        }
    }
}
