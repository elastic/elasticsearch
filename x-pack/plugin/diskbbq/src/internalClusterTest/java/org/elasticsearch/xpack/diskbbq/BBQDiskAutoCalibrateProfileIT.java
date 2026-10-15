/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibrationProfile;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
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

    public void testIndexWithEveryAutoCalibrateProfileIsAppliedAndSearchable() {
        for (IvfAutoCalibrationProfile profile : IvfAutoCalibrationProfile.values()) {
            String index = INDEX + "-" + profile;
            createIndex(index, "\"" + profile + "\"");
            assertAutoCalibrateInMapping(index, profile.toString());
            indexAndSearch(index);
        }
    }

    /** Booleans never need the feature check, so they were never affected; they must keep working next to the profile names. */
    public void testIndexWithBooleanAutoCalibrateIsAppliedAndSearchable() {
        for (boolean autoCalibrate : new boolean[] { false, true }) {
            String index = INDEX + "-" + autoCalibrate;
            createIndex(index, Boolean.toString(autoCalibrate));
            assertAutoCalibrateInMapping(index, autoCalibrate);
            indexAndSearch(index);
        }
    }

    private void createIndex(String index, String autoCalibrateJson) {
        assertAcked(
            prepareCreate(index).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping(Strings.format("""
                    {
                      "properties": {
                        "%s": {
                          "type": "dense_vector",
                          "dims": %d,
                          "index": true,
                          "similarity": "l2_norm",
                          "index_options": { "type": "bbq_disk", "auto_calibrate": %s }
                        }
                      }
                    }""", FIELD, DIMS, autoCalibrateJson))
        );
        ensureGreen(index);
    }

    @SuppressWarnings("unchecked")
    private void assertAutoCalibrateInMapping(String index, Object expected) {
        Map<String, Object> mapping = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, index)
            .get()
            .mappings()
            .get(index)
            .sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> field = (Map<String, Object>) properties.get(FIELD);
        Map<String, Object> indexOptions = (Map<String, Object>) field.get("index_options");
        assertThat(indexOptions.get("auto_calibrate"), equalTo(expected));
    }

    private void indexAndSearch(String index) {
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < DOCS; i++) {
            float[] vector = new float[DIMS];
            for (int d = 0; d < DIMS; d++) {
                vector[d] = randomFloat();
            }
            bulk.add(client().prepareIndex(index).setId(String.valueOf(i)).setSource(FIELD, vector));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        flushAndRefresh(index);

        assertResponse(
            prepareSearch(index).setKnnSearch(List.of(new KnnSearchBuilder(FIELD, new float[DIMS], 5, DOCS, null, null, null))).setSize(5),
            searchResponse -> assertThat(searchResponse.getHits().getHits().length, equalTo(5))
        );
    }
}
