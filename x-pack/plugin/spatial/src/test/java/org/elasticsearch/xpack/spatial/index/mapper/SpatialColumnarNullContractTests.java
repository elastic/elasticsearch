/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.index.mapper.AbstractColumnarNullContractTestCase;
import org.elasticsearch.index.mapper.IndexingStrategy;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SpatialColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    static String wellKnownText() {
        return "POINT (" + (randomIntBetween(0, 100) + 0.5) + " " + (randomIntBetween(0, 80) + 0.5) + ")";
    }

    // NOTE: point, geo_shape and shape does not implement batch indexing today, so the scenario lists no batched shape and the BATCH tests
    // are not
    // required to execute as a batch. They exercise batch indexing as soon as the mapper accepts a column.
    static Scenario point() {
        return scenario("point", () -> Map.of("x", randomIntBetween(0, 100) + 0.5, "y", randomIntBetween(0, 100) + 0.5)).withoutIndex()
            .withCanonical(value -> new TreeMap<>((Map<?, ?>) value).toString());
    }

    static Scenario geoShape() {
        return scenario("geo_shape", SpatialColumnarNullContractTests::wellKnownText).withoutIndex();
    }

    static Scenario shape() {
        return scenario("shape", SpatialColumnarNullContractTests::wellKnownText).withoutIndex();
    }

    public void testPointExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(point(), IndexingStrategy.DOCUMENT);
    }

    public void testPointExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(point(), IndexingStrategy.BATCH);
    }

    public void testPointExistsCountDocument() throws IOException {
        assertExistsCountAgreesWithHits(point(), IndexingStrategy.DOCUMENT);
    }

    public void testPointExistsCountBatch() throws IOException {
        assertExistsCountAgreesWithHits(point(), IndexingStrategy.BATCH);
    }

    public void testPointSourceDocument() throws IOException {
        assertSourceDropsNullElements(point(), IndexingStrategy.DOCUMENT);
    }

    public void testPointSourceBatch() throws IOException {
        assertSourceDropsNullElements(point(), IndexingStrategy.BATCH);
    }

    public void testGeoShapeExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(geoShape(), IndexingStrategy.DOCUMENT);
    }

    public void testGeoShapeExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(geoShape(), IndexingStrategy.BATCH);
    }

    public void testGeoShapeExistsCountDocument() throws IOException {
        assertExistsCountAgreesWithHits(geoShape(), IndexingStrategy.DOCUMENT);
    }

    public void testGeoShapeExistsCountBatch() throws IOException {
        assertExistsCountAgreesWithHits(geoShape(), IndexingStrategy.BATCH);
    }

    public void testGeoShapeSourceDocument() throws IOException {
        assertSourceDropsNullElements(geoShape(), IndexingStrategy.DOCUMENT);
    }

    public void testGeoShapeSourceBatch() throws IOException {
        assertSourceDropsNullElements(geoShape(), IndexingStrategy.BATCH);
    }

    public void testShapeExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(shape(), IndexingStrategy.DOCUMENT);
    }

    public void testShapeExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(shape(), IndexingStrategy.BATCH);
    }

    public void testShapeExistsCountDocument() throws IOException {
        assertExistsCountAgreesWithHits(shape(), IndexingStrategy.DOCUMENT);
    }

    public void testShapeExistsCountBatch() throws IOException {
        assertExistsCountAgreesWithHits(shape(), IndexingStrategy.BATCH);
    }

    public void testShapeSourceDocument() throws IOException {
        assertSourceDropsNullElements(shape(), IndexingStrategy.DOCUMENT);
    }

    public void testShapeSourceBatch() throws IOException {
        assertSourceDropsNullElements(shape(), IndexingStrategy.BATCH);
    }
}
