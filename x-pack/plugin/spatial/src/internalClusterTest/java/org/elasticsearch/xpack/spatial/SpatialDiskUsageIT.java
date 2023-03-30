/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageRequest;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SpatialDiskUsageIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
    }

    public void testGeoShape() throws Exception {
        doTestSpatialField(GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE);
    }

    public void testCartesianShape() throws Exception {
        doTestSpatialField(ShapeFieldMapper.CONTENT_TYPE);
    }

    public void testCartesianPoint() throws Exception {
        doTestSpatialField(PointFieldMapper.CONTENT_TYPE);
    }

    private void doTestSpatialField(String type) throws Exception {
        final XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("location");
                    mapping.field("type", type);
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        final String index = "test-index";
        client().admin()
            .indices()
            .prepareCreate(index)
            .setMapping(mapping)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();

        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            final XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("location")
                .field("type", "point")
                .field("coordinates", new double[] { GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude() })
                .endObject()
                .endObject();
            client().prepareIndex(index).setId("id-" + i).setSource(doc).get();
        }
        AnalyzeIndexDiskUsageResponse resp = client().execute(
            AnalyzeIndexDiskUsageAction.INSTANCE,
            new AnalyzeIndexDiskUsageRequest(new String[] { index }, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, true)
        ).actionGet();

        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        resp.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), BytesReference.bytes(builder).array());
        Map<String, Object> objects = parser.map();
        assertNotNull(objects);

        int value = extractValue("test-index.store_size_in_bytes", objects);
        assertThat(value, greaterThan(100));

        value = extractValue("test-index.fields.location.total_in_bytes", objects);
        assertThat(value, greaterThan(0));

        value = extractValue("test-index.fields.location.points_in_bytes", objects);
        assertThat(value, greaterThan(0));

        value = extractValue("test-index.fields._source.inverted_index.total_in_bytes", objects);
        assertThat(value, equalTo(0));
        value = extractValue("test-index.fields._source.stored_fields_in_bytes", objects);
        assertThat(value, greaterThan(0));
        value = extractValue("test-index.fields._source.points_in_bytes", objects);
        assertThat(value, equalTo(0));
        value = extractValue("test-index.fields._source.doc_values_in_bytes", objects);
        assertThat(value, equalTo(0));
        value = extractValue("test-index.fields._id.inverted_index.total_in_bytes", objects);
        assertThat(value, greaterThan(0));
        value = extractValue("test-index.fields._id.stored_fields_in_bytes", objects);
        assertThat(value, greaterThan(0));
        value = extractValue("test-index.fields._id.points_in_bytes", objects);
        assertThat(value, equalTo(0));
        value = extractValue("test-index.fields._id.doc_values_in_bytes", objects);
        assertThat(value, equalTo(0));

        value = extractValue("test-index.fields._seq_no.inverted_index.total_in_bytes", objects);
        assertThat(value, equalTo(0));
        value = extractValue("test-index.fields._seq_no.stored_fields_in_bytes", objects);
        assertThat(value, equalTo(0));
        value = extractValue("test-index.fields._seq_no.points_in_bytes", objects);
        assertThat(value, greaterThan(0));
        value = extractValue("test-index.fields._seq_no.doc_values_in_bytes", objects);
        assertThat(value, greaterThan(0));
    }

    private int extractValue(String path, Map<String, Object> objects) {
        Object o = XContentMapValues.extractValue(path, objects);
        assertTrue(o instanceof Integer);
        return (Integer) o;
    }
}
