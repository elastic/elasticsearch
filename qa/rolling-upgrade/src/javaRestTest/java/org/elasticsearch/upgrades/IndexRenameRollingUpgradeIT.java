/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import io.netty.handler.codec.http.HttpMethod;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * A node that does not know about renames would treat a renamed index as deleted and delete its shards, so renames must be
 * refused until every node in the cluster supports them, and must work once the upgrade is complete.
 */
public class IndexRenameRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static final String INDEX_NAME = "test_rename_upgrade";
    private static final String RENAMED_INDEX_NAME = "test_rename_upgrade_renamed";

    public IndexRenameRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testRenameOnlyAfterUpgrade() throws Exception {
        if (isOldCluster()) {
            createIndex(INDEX_NAME);
            client().performRequest(
                newXContentRequest(HttpMethod.PUT, "/" + INDEX_NAME + "/_doc/1?refresh=true", (builder, params) -> builder.field("f", "v"))
            );
        } else if (isMixedCluster()) {
            // an old node has no rename endpoint and a new node refuses because not every node supports renames; either way the
            // request fails and the index is untouched
            expectThrows(ResponseException.class, () -> rename(INDEX_NAME, RENAMED_INDEX_NAME));
            assertThat(docCount(INDEX_NAME), equalTo(1));
            assertFalse(indexExists(RENAMED_INDEX_NAME));
        } else {
            assertTrue(isUpgradedCluster());
            rename(INDEX_NAME, RENAMED_INDEX_NAME);
            assertFalse(indexExists(INDEX_NAME));
            assertThat(docCount(RENAMED_INDEX_NAME), equalTo(1));
        }
    }

    private static void rename(String source, String target) throws IOException {
        client().performRequest(new Request(HttpMethod.POST.name(), "/" + source + "/_rename/" + target));
    }

    private static int docCount(String index) throws IOException {
        final Map<String, Object> response = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_count")));
        return (Integer) XContentMapValues.extractValue("count", response);
    }

}
