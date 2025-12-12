/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.diskbbq.LocalStateDiskBBQ;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@LuceneTestCase.SuppressCodecs("*")
public class DiskBBQLicensingIT extends ESIntegTestCase {

    @Before
    public void resetLicensing() {
        enableLicensing();

        ensureStableCluster(1);
        ensureYellow();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        return settings.build();
    }

    public void testCreateDiskBBQIndexRestricted() {
        // disable any license
        disableLicensing();
        // doesn't throw
        client().admin().indices().prepareCreate("diskbbq-index").setMapping("""
              {
              "properties": {
                "vector": {
                  "type": "dense_vector",
                  "index_options": {
                    "type": "bbq_disk"
                  }
                }
              }
            }""").get();
        // throws on write
        var e = expectThrows(ElasticsearchException.class, () -> client().prepareIndex("diskbbq-index").setSource("""
              {
                "vector": [0.1, 0.2, 0.3, 0.4]
              }
            """, XContentType.JSON).get());
        assertTrue(e.getMessage().contains("current license is non-compliant"));
        // apply an invalid license
        enableLicensing(randomInvalidLicenseType());
        e = expectThrows(ElasticsearchException.class, () -> client().prepareIndex("diskbbq-index").setSource("""
              {
                "vector": [0.1, 0.2, 0.3, 0.4]
              }
            """, XContentType.JSON).get());
        assertTrue(e.getMessage().contains("current license is non-compliant"));
    }

    public void testCreateDiskBBQIndexUnrestricted() {
        enableLicensing(randomValidLicenseType());
        // doesn't throw
        client().admin().indices().prepareCreate("diskbbq-index").setMapping("""
              {
              "properties": {
                "vector": {
                  "type": "dense_vector",
                  "index_options": {
                    "type": "bbq_disk"
                  }
                }
              }
            }""").get();
        // doesn't throw on write
        client().prepareIndex("diskbbq-index").setSource("""
              {
                "vector": [0.1, 0.2, 0.3, 0.4]
              }
            """, XContentType.JSON).get();
    }

    public void testReadDiskBBQIndexWithLicenseChange() {
        enableLicensing(randomValidLicenseType());
        // create index and write doc
        client().admin().indices().prepareCreate("diskbbq-index").setMapping("""
              {
              "properties": {
                "vector": {
                  "type": "dense_vector",
                  "index_options": {
                    "type": "bbq_disk"
                  }
                }
              }
            }""").get();
        client().prepareIndex("diskbbq-index").setSource("""
              {
                "vector": [0.1, 0.2, 0.3, 0.4]
              }
            """, XContentType.JSON).get();

        final var ksb = new KnnSearchBuilder("vector", new float[] { 0.1f, 0.2f, 0.3f, 0.4f }, 10, 10, null, null, null);
        // valid license, should not throw
        assertNoFailures(client().prepareSearch("diskbbq-index").setKnnSearch(List.of(ksb)));
        disableLicensing();
        assertNoFailures(client().prepareSearch("diskbbq-index").setKnnSearch(List.of(ksb)));
        enableLicensing(randomInvalidLicenseType());
        assertNoFailures(client().prepareSearch("diskbbq-index").setKnnSearch(List.of(ksb)));
    }

    private static License.OperationMode randomInvalidLicenseType() {
        return randomFrom(
            License.OperationMode.GOLD,
            License.OperationMode.STANDARD,
            License.OperationMode.BASIC,
            License.OperationMode.PLATINUM
        );
    }

    public static void enableLicensing() {
        enableLicensing(randomValidLicenseType());
    }

    public static void disableLicensing() {
        disableLicensing(randomValidLicenseType());
    }

    public static void disableLicensing(License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            licenseState.update(new XPackLicenseStatus(operationMode, false, null));
        }
    }

    private static License.OperationMode randomValidLicenseType() {
        return randomFrom(License.OperationMode.TRIAL, License.OperationMode.ENTERPRISE);
    }

    public static void enableLicensing(License.OperationMode operationMode) {
        for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
            licenseState.update(new XPackLicenseStatus(operationMode, true, null));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateDiskBBQ.class);
    }
}
