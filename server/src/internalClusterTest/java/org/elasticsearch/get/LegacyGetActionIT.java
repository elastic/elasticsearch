/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import static org.elasticsearch.get.GetActionIT.indexOrAlias;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class LegacyGetActionIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testGetFieldsMetadataWithRouting() throws Exception {
        assertAcked(
            prepareCreate("test").addMapping("_doc", "field1", "type=keyword,store=true")
                .addAlias(new Alias("alias"))
                .setSettings(
                    Settings.builder()
                        .put("index.refresh_interval", -1)
                        .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_6_0_0)
                )
        ); // multi-types in 6.0.0

        try (XContentBuilder source = jsonBuilder().startObject().field("field1", "value").endObject()) {
            client().prepareIndex("test", "_doc", "1").setRouting("1").setSource(source).get();
        }

        {
            final GetResponse getResponse = client().prepareGet(indexOrAlias(), "_doc", "1")
                .setRouting("1")
                .setStoredFields("field1")
                .get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
            assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
        }

        flush();

        {
            final GetResponse getResponse = client().prepareGet(indexOrAlias(), "_doc", "1")
                .setStoredFields("field1")
                .setRouting("1")
                .get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
            assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
        }
    }

}
