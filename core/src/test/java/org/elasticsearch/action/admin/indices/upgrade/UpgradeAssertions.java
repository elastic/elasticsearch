/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.upgrade.get.IndexUpgradeStatus;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.engine.Segment;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UpgradeAssertions {
    private static Collection<IndexUpgradeStatus> getUpgradeStatus(Client client, String... indices) throws Exception {
        UpgradeStatusResponse upgradeStatusResponse = client.admin().indices().prepareUpgradeStatus(indices).get();
        assertNoFailures(upgradeStatusResponse);
        return upgradeStatusResponse.getIndices().values();
    }

    public static void assertNotUpgraded(Client client, String... index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            assertTrue("index " + status.getIndex() + " should not be zero sized", status.getTotalBytes() != 0);
            // TODO: it would be better for this to be strictly greater, but sometimes an extra flush
            // mysteriously happens after the second round of docs are indexed
            assertTrue("index " + status.getIndex() + " should have recovered some segments from transaction log",
                       status.getTotalBytes() >= status.getToUpgradeBytes());
            assertTrue("index " + status.getIndex() + " should need upgrading", status.getToUpgradeBytes() != 0);
        }
    }

    public static void assertUpgraded(Client client, String... index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            assertTrue("index " + status.getIndex() + " should not be zero sized", status.getTotalBytes() != 0);
            assertEquals("index " + status.getIndex() + " should be upgraded",
                0, status.getToUpgradeBytes());
        }

        // double check using the segments api that all segments are actually upgraded
        IndicesSegmentResponse segsRsp;
        if (index == null) {
            segsRsp = client.admin().indices().prepareSegments().execute().actionGet();
        } else {
            segsRsp = client.admin().indices().prepareSegments(index).execute().actionGet();
        }
        for (IndexSegments indexSegments : segsRsp.getIndices().values()) {
            for (IndexShardSegments shard : indexSegments) {
                for (ShardSegments segs : shard.getShards()) {
                    for (Segment seg : segs.getSegments()) {
                        assertEquals("Index " + indexSegments.getIndex() + " has unupgraded segment " + seg.toString(),
                                     Version.CURRENT.luceneVersion.major, seg.version.major);
                        assertEquals("Index " + indexSegments.getIndex() + " has unupgraded segment " + seg.toString(),
                                     Version.CURRENT.luceneVersion.minor, seg.version.minor);
                    }
                }
            }
        }
    }

    public static void assertNoAncientSegments(Client client, String... index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            assertTrue("index " + status.getIndex() + " should not be zero sized", status.getTotalBytes() != 0);
            // TODO: it would be better for this to be strictly greater, but sometimes an extra flush
            // mysteriously happens after the second round of docs are indexed
            assertTrue("index " + status.getIndex() + " should not have any ancient segments",
                       status.getToUpgradeBytesAncient() == 0);
            assertTrue("index " + status.getIndex() + " should have recovered some segments from transaction log",
                       status.getTotalBytes() >= status.getToUpgradeBytes());
            assertTrue("index " + status.getIndex() + " should need upgrading", status.getToUpgradeBytes() != 0);
        }
    }

    /** Returns true if there are any old but not ancient segments. */
    public static boolean hasOldButNotAncientSegments(Client client, String index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            if (status.getToUpgradeBytes() > status.getToUpgradeBytesAncient()) {
                return true;
            }
        }
        return false;
    }

    public static boolean isUpgraded(Client client, String index) throws Exception {
        ESLogger logger = Loggers.getLogger(UpgradeAssertions.class);
        int toUpgrade = 0;
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            logger.info("Index: " + status.getIndex() + ", total: " + status.getTotalBytes() + ", toUpgrade: " + status.getToUpgradeBytes());
            toUpgrade += status.getToUpgradeBytes();
        }
        return toUpgrade == 0;
    }

    /** Returns true if there are any ancient segments. */
    public static boolean hasAncientSegments(Client client, String index) throws Exception {
        for (IndexUpgradeStatus status : getUpgradeStatus(client, index)) {
            if (status.getToUpgradeBytesAncient() != 0) {
                return true;
            }
        }
        return false;
    }
}
