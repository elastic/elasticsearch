/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;

public class MappingIT extends AbstractRollingTestCase {
    /**
     * Create a mapping that explicitly disables the _all field (possible in 6x, see #37429)
     * and check that it can be upgraded to 7x.
     */
    public void testAllFieldDisable6x() throws Exception {
        assumeTrue("_all", UPGRADE_FROM_VERSION.before(Version.V_7_0_0));
        switch (CLUSTER_TYPE) {
            case OLD:
                Request createTestIndex = new Request("PUT", "all-index");
                createTestIndex.addParameter("include_type_name", "false");
                createTestIndex.setJsonEntity(
                    "{ \"settings\": { \"index.number_of_shards\": 1 }, "
                        + "\"mappings\": {\"_all\": { \"enabled\": false }, \"properties\": { \"field\": { \"type\": \"text\" }}}}"
                );
                createTestIndex.setOptions(
                    expectWarnings(
                        "[_all] is deprecated in 6.0+ and will be removed in 7.0. As a replacement,"
                            + " "
                            + "you can use [copy_to] on mapping fields to create your own catch all field."
                    )
                );
                Response resp = client().performRequest(createTestIndex);
                assertEquals(200, resp.getStatusLine().getStatusCode());
                break;

            default:
                final Request request = new Request("GET", "all-index");
                Response response = client().performRequest(request);
                assertEquals(200, response.getStatusLine().getStatusCode());
                Object enabled = XContentMapValues.extractValue("all-index.mappings._all.enabled", entityAsMap(response));
                assertNotNull(enabled);
                assertEquals(false, enabled);
                break;
        }
    }

    public void testMapperDynamicIndexSetting() throws IOException {
        assumeTrue("Setting not removed before 7.0", UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_0_0));
        switch (CLUSTER_TYPE) {
            case OLD:
                createIndex("my-index", Settings.EMPTY);

                Request request = new Request("PUT", "/my-index/_settings");
                request.setJsonEntity(
                    Strings.toString(Settings.builder().put("index.mapper.dynamic", true).put("index.number_of_replicas", 2).build())
                );
                request.setOptions(
                    expectWarnings(
                        "[index.mapper.dynamic] setting was deprecated in Elasticsearch and will be removed in a future release! "
                            + "See the breaking changes documentation for the next major version."
                    )
                );
                assertOK(client().performRequest(request));
                break;
            case MIXED:
                ensureHealth(r -> {
                    r.addParameter("wait_for_status", "yellow");
                    r.addParameter("wait_for_no_relocating_shards", "true");
                });
                break;
            case UPGRADED:
                // During the upgrade my-index shards may be allocated to not upgraded nodes, these then fail to allocate.
                // If allocation fails more than 5 times, allocation is not retried immediately, this reroute triggers allocation
                // any failed allocations. So that my-index health will be green.
                Request rerouteRequest = new Request("POST", "cluster/reroute");
                rerouteRequest.addParameter("retry_failed", "true");
                assertOK(client().performRequest(rerouteRequest));
                ensureGreen("my-index");
                break;
        }
    }

}
