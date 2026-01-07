/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SimpleFeatureFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.script.ScriptCompiler;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class GeoPointFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        boolean ignoreMalformed = randomBoolean();
        MappedFieldType mapper = new GeoPointFieldMapper.Builder("field", ScriptCompiler.NONE, defaultIndexSettings()).ignoreMalformed(
            ignoreMalformed
        ).build(MapperBuilderContext.root(false, false)).fieldType();

        Map<String, Object> jsonPoint = Map.of("type", "Point", "coordinates", List.of(42.0, 27.1));
        Map<String, Object> otherJsonPoint = Map.of("type", "Point", "coordinates", List.of(30.0, 50.0));
        String wktPoint = "POINT (42.0 27.1)";
        String otherWktPoint = "POINT (30.0 50.0)";
        byte[] wkbPoint = WellKnownBinary.toWKB(new Point(42.0, 27.1), ByteOrder.LITTLE_ENDIAN);
        byte[] otherWkbPoint = WellKnownBinary.toWKB(new Point(30.0, 50.0), ByteOrder.LITTLE_ENDIAN);

        // Test a single point in [lon, lat] array format.
        Object sourceValue = List.of(42.0, 27.1);
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        List<?> wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(1));
        assertThat(wkb.get(0), equalTo(wkbPoint));

        // Test a single point in "lat, lon" string format.
        sourceValue = "27.1,42.0";
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(1));
        assertThat(wkb.get(0), equalTo(wkbPoint));

        // Test a list of points in [lon, lat] array format.
        sourceValue = List.of(List.of(42.0, 27.1), List.of(30.0, 50.0));
        assertEquals(List.of(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(2));
        assertThat(wkb.get(0), equalTo(wkbPoint));
        assertThat(wkb.get(1), equalTo(otherWkbPoint));

        // Test a list of points in [lat,lon] array format with one malformed
        sourceValue = List.of(List.of(42.0, 27.1), List.of("a", "b"), List.of(30.0, 50.0));
        assertEquals(List.of(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(2));
        assertThat(wkb.get(0), equalTo(wkbPoint));
        assertThat(wkb.get(1), equalTo(otherWkbPoint));

        // Test a single point in well-known text format.
        sourceValue = "POINT (42.0 27.1)";
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
        wkb = fetchSourceValue(mapper, sourceValue, "wkb");
        assertThat(wkb.size(), equalTo(1));
        assertThat(wkb.get(0), equalTo(wkbPoint));

        // Test a malformed value
        sourceValue = "malformed";
        assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, null));

        // test normalize
        sourceValue = "27.1,402.0";
        if (ignoreMalformed) {
            assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
            assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
            wkb = fetchSourceValue(mapper, sourceValue, "wkb");
            assertThat(wkb.size(), equalTo(1));
            assertThat(wkb.get(0), equalTo(wkbPoint));
        } else {
            assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, null));
            assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkt"));
            assertEquals(List.of(), fetchSourceValue(mapper, sourceValue, "wkb"));
        }

        // test single point in GeoJSON format
        sourceValue = jsonPoint;
        assertEquals(List.of(jsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));

        // Test a list of points in GeoJSON format
        sourceValue = List.of(jsonPoint, otherJsonPoint);
        assertEquals(List.of(jsonPoint, otherJsonPoint), fetchSourceValue(mapper, sourceValue, null));
        assertEquals(List.of(wktPoint, otherWktPoint), fetchSourceValue(mapper, sourceValue, "wkt"));
    }

    public void testFetchVectorTile() throws IOException {
        MappedFieldType mapper = new GeoPointFieldMapper.Builder("field", ScriptCompiler.NONE, defaultIndexSettings()).build(
            MapperBuilderContext.root(false, false)
        ).fieldType();
        final int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        final SimpleFeatureFactory featureFactory;
        final String mvtString;
        if (randomBoolean()) {
            int extent = randomIntBetween(1 << 8, 1 << 14);
            mvtString = "mvt(" + z + "/" + x + "/" + y + "@" + extent + ")";
            featureFactory = new SimpleFeatureFactory(z, x, y, extent);
        } else {
            mvtString = "mvt(" + z + "/" + x + "/" + y + ")";
            featureFactory = new SimpleFeatureFactory(z, x, y, 4096);
        }
        List<GeoPoint> geoPoints = new ArrayList<>();
        List<List<Double>> values = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            final double lat = GeoTestUtil.nextLatitude();
            final double lon = GeoTestUtil.nextLongitude();
            List<?> sourceValue = fetchSourceValue(mapper, List.of(lon, lat), mvtString);
            assertThat(sourceValue.size(), equalTo(1));
            assertThat(sourceValue.get(0), equalTo(featureFactory.point(lon, lat)));
            geoPoints.add(new GeoPoint(lat, lon));
            values.add(List.of(lon, lat));
        }
        List<?> sourceValue = fetchSourceValue(mapper, values, mvtString);
        assertThat(sourceValue.size(), equalTo(1));
        assertThat(sourceValue.get(0), equalTo(featureFactory.points(geoPoints)));
    }

    public void testBlockLoaderWhenDocValuesAreEnabledAndThePreferenceIsToUseDocValues() {
        // given
        GeoPointFieldMapper.GeoPointFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("potato");
        MappedFieldType.BlockLoaderContext blContextMock = mockBlockLoaderContext(false, MappedFieldType.FieldExtractPreference.DOC_VALUES);

        // when
        BlockLoader loader = fieldType.blockLoader(blContextMock);

        // then
        // verify that we use the correct block value reader
        assertThat(loader, instanceOf(LongsBlockLoader.class));
    }

    public void testBlockLoaderWhenDocValuesAreEnabledAndThereIsNoPreference() {
        // given
        GeoPointFieldMapper.GeoPointFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("potato");
        MappedFieldType.BlockLoaderContext blContextMock = mockBlockLoaderContext(false, MappedFieldType.FieldExtractPreference.NONE);

        // when
        BlockLoader loader = fieldType.blockLoader(blContextMock);

        // then
        // verify that we use the correct block value reader
        assertThat(loader, instanceOf(BlockSourceReader.GeometriesBlockLoader.class));
    }

    public void testBlockLoaderWhenFieldIsStoredAndThePreferenceIsToUseStoredFields() {
        // given
        GeoPointFieldMapper.GeoPointFieldType fieldType = createFieldType(true, false, false);
        MappedFieldType.BlockLoaderContext blContextMock = mockBlockLoaderContext(false, MappedFieldType.FieldExtractPreference.STORED);

        // when
        BlockLoader loader = fieldType.blockLoader(blContextMock);

        // then
        // verify that we use the correct block value reader
        assertThat(loader, instanceOf(BlockSourceReader.GeometriesBlockLoader.class));
    }

    public void testBlockLoaderWhenFieldIsStoredAndThereIsNoPreference() {
        // given
        GeoPointFieldMapper.GeoPointFieldType fieldType = createFieldType(true, false, false);
        MappedFieldType.BlockLoaderContext blContextMock = mockBlockLoaderContext(false, MappedFieldType.FieldExtractPreference.NONE);

        // when
        BlockLoader loader = fieldType.blockLoader(blContextMock);

        // then
        // verify that we use the correct block value reader
        assertThat(loader, instanceOf(BlockSourceReader.GeometriesBlockLoader.class));
    }

    public void testBlockLoaderWhenSyntheticSourceIsEnabledAndFieldIsStoredInIgnoredSource() {
        // given
        GeoPointFieldMapper.GeoPointFieldType fieldType = createFieldType(false, false, true);
        MappedFieldType.BlockLoaderContext blContextMock = mockBlockLoaderContext(true, MappedFieldType.FieldExtractPreference.NONE);

        // when
        BlockLoader loader = fieldType.blockLoader(blContextMock);

        // then
        // verify that we use the correct block value reader
        assertThat(loader, instanceOf(FallbackSyntheticSourceBlockLoader.class));
    }

    public void testBlockLoaderWhenSyntheticSourceAndDocValuesAreEnabled() {
        // given
        GeoPointFieldMapper.GeoPointFieldType fieldType = createFieldType(false, true, true);
        MappedFieldType.BlockLoaderContext blContextMock = mockBlockLoaderContext(true, MappedFieldType.FieldExtractPreference.NONE);

        // when
        BlockLoader loader = fieldType.blockLoader(blContextMock);

        // then
        // verify that we use the correct block value reader
        assertThat(loader, instanceOf(GeoPointFieldMapper.BytesRefFromLongsBlockLoader.class));
    }

    public void testBlockLoaderFallsBackToSource() {
        // given
        GeoPointFieldMapper.GeoPointFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("potato");
        MappedFieldType.BlockLoaderContext blContextMock = mockBlockLoaderContext(
            false,
            MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS
        );

        // when
        BlockLoader loader = fieldType.blockLoader(blContextMock);

        // then
        // verify that we use the correct block value reader
        assertThat(loader, instanceOf(BlockSourceReader.GeometriesBlockLoader.class));
    }

    private MappedFieldType.BlockLoaderContext mockBlockLoaderContext(
        boolean enableSyntheticSource,
        MappedFieldType.FieldExtractPreference fieldExtractPreference
    ) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1);

        if (enableSyntheticSource) {
            builder.put("index.mapping.source.mode", "synthetic");
        }

        Settings settings = builder.build();

        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("index").settings(builder).build(), settings);

        MappedFieldType.BlockLoaderContext blContextMock = mock(MappedFieldType.BlockLoaderContext.class);
        doReturn(fieldExtractPreference).when(blContextMock).fieldExtractPreference();
        doReturn(indexSettings).when(blContextMock).indexSettings();

        return blContextMock;
    }

    private GeoPointFieldMapper.GeoPointFieldType createFieldType(
        boolean isStored,
        boolean hasDocValues,
        boolean isSyntheticSourceEnabled
    ) {
        return new GeoPointFieldMapper.GeoPointFieldType(
            "potato",
            hasDocValues ? IndexType.docValuesOnly() : IndexType.NONE,
            isStored,
            null,
            null,
            null,
            Collections.emptyMap(),
            TimeSeriesParams.MetricType.COUNTER,
            IndexMode.LOGSDB,
            isSyntheticSourceEnabled
        );
    }

}
