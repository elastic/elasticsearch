/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper.size;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.plugin.mapper.MapperSizePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SizeMappingIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MapperSizePlugin.class);
    }

    // issue 5053
    public void testThatUpdatingMappingShouldNotRemoveSizeMappingConfiguration() throws Exception {
        String index = "foo";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_size").field("enabled", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        // check mapping again
        assertSizeMappingEnabled(index, true);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("otherField")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();
        AcknowledgedResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure size field is still in mapping
        assertSizeMappingEnabled(index, true);
    }

    public void testThatSizeCanBeSwitchedOnAndOff() throws Exception {
        String index = "foo";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_size").field("enabled", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        // check mapping again
        assertSizeMappingEnabled(index, true);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject()
            .startObject("_size")
            .field("enabled", false)
            .endObject()
            .endObject();
        AcknowledgedResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure size field is still in mapping
        assertSizeMappingEnabled(index, false);
    }

    private void assertSizeMappingEnabled(String index, boolean enabled) throws IOException {
        String errMsg = String.format(
            Locale.ROOT,
            "Expected size field mapping to be " + (enabled ? "enabled" : "disabled") + " for %s",
            index
        );
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).get();
        Map<String, Object> mappingSource = getMappingsResponse.getMappings().get(index).getSourceAsMap();
        assertThat(errMsg, mappingSource, hasKey("_size"));
        String sizeAsString = mappingSource.get("_size").toString();
        assertThat(sizeAsString, is(notNullValue()));
        assertThat(errMsg, sizeAsString, is("{enabled=" + (enabled) + "}"));
    }

    public void testBasic() throws Exception {
        assertAcked(prepareCreate("test").setMapping("_size", "enabled=true"));
        final String source = "{\"f\":\"" + randomAlphaOfLengthBetween(1, 100) + "\"}";
        indexRandom(true, client().prepareIndex("test").setId("1").setSource(source, XContentType.JSON));
        GetResponse getResponse = client().prepareGet("test", "1").setStoredFields("_size").get();
        assertNotNull(getResponse.getField("_size"));
        assertEquals(source.length(), (int) getResponse.getField("_size").getValue());
    }

    public void testGetWithFields() throws Exception {
        assertAcked(prepareCreate("test").setMapping("_size", "enabled=true"));
        final String source = "{\"f\":\"" + randomAlphaOfLengthBetween(1, 100) + "\"}";
        indexRandom(true, client().prepareIndex("test").setId("1").setSource(source, XContentType.JSON));
        SearchResponse searchResponse = client().prepareSearch("test").addFetchField("_size").get();
        assertEquals(source.length(), ((Long) searchResponse.getHits().getHits()[0].getFields().get("_size").getValue()).intValue());

        // this should not work when requesting fields via wildcard expression
        searchResponse = client().prepareSearch("test").addFetchField("*").get();
        assertNull(searchResponse.getHits().getHits()[0].getFields().get("_size"));

        // This should STILL work
        searchResponse = client().prepareSearch("test").addStoredField("*").get();
        assertNotNull(searchResponse.getHits().getHits()[0].getFields().get("_size"));
    }

    public void testWildCardWithFieldsWhenDisabled() throws Exception {
        assertAcked(prepareCreate("test").setMapping("_size", "enabled=false"));
        final String source = "{\"f\":\"" + randomAlphaOfLengthBetween(1, 100) + "\"}";
        indexRandom(true, client().prepareIndex("test").setId("1").setSource(source, XContentType.JSON));
        SearchResponse searchResponse = client().prepareSearch("test").addFetchField("_size").get();
        assertNull(searchResponse.getHits().getHits()[0].getFields().get("_size"));

        searchResponse = client().prepareSearch("test").addFetchField("*").get();
        assertNull(searchResponse.getHits().getHits()[0].getFields().get("_size"));

        searchResponse = client().prepareSearch("test").addStoredField("*").get();
        assertNull(searchResponse.getHits().getHits()[0].getFields().get("_size"));
    }

    public void testWildCardWithFieldsWhenNotProvided() throws Exception {
        assertAcked(prepareCreate("test"));
        final String source = "{\"f\":\"" + randomAlphaOfLengthBetween(1, 100) + "\"}";
        indexRandom(true, client().prepareIndex("test").setId("1").setSource(source, XContentType.JSON));
        SearchResponse searchResponse = client().prepareSearch("test").addFetchField("_size").get();
        assertNull(searchResponse.getHits().getHits()[0].getFields().get("_size"));

        searchResponse = client().prepareSearch("test").addFetchField("*").get();
        assertNull(searchResponse.getHits().getHits()[0].getFields().get("_size"));

        searchResponse = client().prepareSearch("test").addStoredField("*").get();
        assertNull(searchResponse.getHits().getHits()[0].getFields().get("_size"));
    }
}
