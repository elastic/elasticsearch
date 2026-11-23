/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibrationProfile;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.index.mapper.vectors.DenseVectorMappingBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.codec.vectors.VectorTestUtils.randomFloatVector;
import static org.elasticsearch.license.DiskBBQLicensingIT.enableLicensing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code auto_calibrate} accepts an auto-calibration profile name as well as a boolean. Parsing a profile name asks whether every node
 * in the cluster supports profiles. That check is made by the master when it validates the mapping; data nodes then apply the persisted
 * mapping on the cluster applier thread, where the cluster state cannot be read to answer it. These tests create indices on a real node
 * with every profile name, so a node that applies the mapping by asking the question again fails here.
 */
@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class BBQDiskAutoCalibrateProfileIT extends ESIntegTestCase {

    private static final String INDEX = "bbq-disk-auto-calibrate-profile";
    private static final String FIELD = "vector";
    private static final int DIMS = 8;
    private static final int DOCS = 20;

    @Before
    public void resetLicensing() {
        enableLicensing();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateDiskBBQ.class);
    }

    public void testIndexWithEveryAutoCalibrateProfileIsAppliedAndSearchable() throws IOException {
        for (IvfAutoCalibrationProfile profile : IvfAutoCalibrationProfile.values()) {
            String index = INDEX + "-" + profile;
            createIndex(index, profile.toString());
            assertAutoCalibrateInMapping(index, profile.toString());
            indexAndSearch(index);
        }
    }

    /** Booleans never need the feature check, so they were never affected; they must keep working next to the profile names. */
    public void testIndexWithBooleanAutoCalibrateIsAppliedAndSearchable() throws IOException {
        for (boolean autoCalibrate : new boolean[] { false, true }) {
            String index = INDEX + "-" + autoCalibrate;
            createIndex(index, autoCalibrate);
            assertAutoCalibrateInMapping(index, autoCalibrate);
            indexAndSearch(index);
        }
    }

    private void createIndex(String index, Object autoCalibrate) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject(FIELD);
        new DenseVectorMappingBuilder().dims(DIMS)
            .index(true)
            .similarity(VectorSimilarity.L2_NORM)
            .indexOptions(Map.of("type", "bbq_disk", "auto_calibrate", autoCalibrate))
            .build(mapping);
        mapping.endObject().endObject().endObject();
        assertAcked(prepareCreate(index).setSettings(indexSettings(1, 0)).setMapping(mapping));
        ensureGreen(index);
    }

    private void assertAutoCalibrateInMapping(String index, Object expected) {
        Map<String, Object> mapping = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, index)
            .get()
            .mappings()
            .get(index)
            .sourceAsMap();
        assertThat(XContentMapValues.extractValue("properties." + FIELD + ".index_options.auto_calibrate", mapping), equalTo(expected));
    }

    private void indexAndSearch(String index) {
        List<IndexRequestBuilder> docs = new ArrayList<>(DOCS);
        for (int i = 0; i < DOCS; i++) {
            docs.add(prepareIndex(index).setId(String.valueOf(i)).setSource(FIELD, randomFloatVector(DIMS)));
        }
        indexRandom(true, docs);

        assertResponse(
            prepareSearch(index).setKnnSearch(List.of(new KnnSearchBuilder(FIELD, new float[DIMS], 5, DOCS, null, null, null))).setSize(5),
            searchResponse -> assertThat(searchResponse.getHits().getHits().length, equalTo(5))
        );
    }
}
