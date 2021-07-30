/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

public class FieldDataLoadingIT extends ESIntegTestCase {

    public void testEagerGlobalOrdinalsFieldDataLoading() throws Exception {
        assertAcked(prepareCreate("test")
                .setMapping(jsonBuilder().startObject().startObject("_doc").startObject("properties")
                        .startObject("name")
                        .field("type", "text")
                        .field("fielddata", true)
                        .field("eager_global_ordinals", true)
                        .endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "name").get();
        client().admin().indices().prepareRefresh("test").get();

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), greaterThan(0L));
    }

}
