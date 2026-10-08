/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Synthetic source layer that rebuilds a {@code shape}/{@code geo_shape} field's {@code _source} from the geometries
 * kept in its field-owned doc value ({@link GeometrySourceDocValuesField}). Geometries are emitted as WKT, in input
 * order.
 */
public class GeometrySourceSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private final String name;
    private BinaryDocValues docValues;
    private List<Geometry> geometries = List.of();

    public GeometrySourceSyntheticFieldLoaderLayer(String name) {
        this.name = name;
    }

    @Override
    public long valueCount() {
        return geometries.size();
    }

    @Override
    public SourceLoader.SyntheticFieldLoader.DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        docValues = leafReader.getBinaryDocValues(name);
        if (docValues == null) {
            geometries = List.of();
            return null;
        }
        return docId -> {
            if (docValues.advanceExact(docId)) {
                geometries = GeometrySourceDocValuesField.decode(docValues.binaryValue());
                return geometries.isEmpty() == false;
            }
            geometries = List.of();
            return false;
        };
    }

    @Override
    public boolean hasValue() {
        return geometries.isEmpty() == false;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        for (Geometry geometry : geometries) {
            b.value(WellKnownText.toWKT(geometry));
        }
    }

    @Override
    public String fieldName() {
        return name;
    }
}
