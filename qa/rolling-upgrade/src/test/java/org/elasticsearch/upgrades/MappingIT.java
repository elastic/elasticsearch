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
import org.elasticsearch.common.xcontent.support.XContentMapValues;

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
}
