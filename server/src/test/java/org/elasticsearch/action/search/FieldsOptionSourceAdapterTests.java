/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

public class FieldsOptionSourceAdapterTests extends ESTestCase {

    /**
     * test that with set fields pattern, the adapter does something.
     * check correct request and response modifications
     */
    public void testFieldsAdapterNoSource() {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchField("foo*");
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(source);
        searchRequest.setFieldsOptionEmulationEnabled(true);
        FieldsOptionSourceAdapter adapter = new FieldsOptionSourceAdapter(searchRequest);

        SetOnce<SearchSourceBuilder> rewrittenSource = new SetOnce<>();
        Version connectionVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3);
        adapter.adaptRequest(connectionVersion, rewrittenSource::set);
        assertNull(rewrittenSource.get().fetchSource());
        assertFalse(adapter.getRemoveSourceOnResponse());

        SearchHit hit = new SearchHit(1).sourceRef(new BytesArray("{ \"foo\" : \"bar\", \"fuzz\":2, \"foo.bar\":[4,5]}"));
        SearchHit[] searchHits = new SearchHit[] { hit };
        assertNull(hit.getFields().get("foo"));
        assertNull(hit.getFields().get("foo.bar"));
        assertNull(hit.getFields().get("fuzz"));
        adapter.adaptResponse(connectionVersion, searchHits);
        assertEquals(1, hit.getFields().get("foo").getValues().size());
        assertEquals("bar", hit.getFields().get("foo").getValues().get(0));
        assertEquals(2, hit.getFields().get("foo.bar").getValues().size());
        assertEquals(4, hit.getFields().get("foo.bar").getValues().get(0));
        assertEquals(5, hit.getFields().get("foo.bar").getValues().get(1));
        assertNull(hit.getFields().get("fuzz"));
        assertTrue(hit.hasSource());
    }

    /**
     * Test that if request source is set to "false", we still set the source on the adapted request and remove it on return
     */
    public void testFieldsAdapterSourceFalse() {
        FieldsOptionSourceAdapter adapter = createAdapter("foo*", false);
        SetOnce<SearchSourceBuilder> rewrittenSource = new SetOnce<>();
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3);
        adapter.adaptRequest(version, rewrittenSource::set);
        assertNotNull(rewrittenSource.get().fetchSource());
        assertTrue(rewrittenSource.get().fetchSource().fetchSource());
        assertEquals(1, rewrittenSource.get().fetchSource().includes().length);
        assertEquals("foo*", rewrittenSource.get().fetchSource().includes()[0]);
        assertTrue(adapter.getRemoveSourceOnResponse());

        SearchHit hit = new SearchHit(1).sourceRef(new BytesArray("{ \"foo\" : \"bar\", \"fuzz\":2, \"foo.bar\":[4,5]}"));
        assertTrue(hit.hasSource());
        SearchHit[] searchHits = new SearchHit[] { hit };
        adapter.adaptResponse(version, searchHits);
        assertFalse(hit.hasSource());
    }

    public void testFieldsAdapterSourceTrue() {
        FieldsOptionSourceAdapter adapter = createAdapter("foo*");
        SetOnce<SearchSourceBuilder> rewrittenSource = new SetOnce<>();
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3);
        adapter.adaptRequest(version, rewrittenSource::set);
        assertFalse(adapter.getRemoveSourceOnResponse());

        SearchHit hit = new SearchHit(1).sourceRef(new BytesArray("{ \"foo\" : \"bar\", \"fuzz\":2, \"foo.bar\":[4,5]}"));
        SearchHit[] searchHits = new SearchHit[] { hit };
        adapter.adaptResponse(version, searchHits);
        assertTrue(hit.hasSource());
    }

    /**
     * while querying the root or object fields works in "_source" filtering, we should not
     * return this via the fields API, since our regular lookup there only returns flattened leaf values
     */
    public void testFieldsAdapterObjects() {
        FieldsOptionSourceAdapter adapter = createAdapter("obj");

        SearchHit hit = new SearchHit(1).sourceRef(new BytesArray("{ \"obj\": { \"foo\" : \"value\"}}"));
        SearchHit[] searchHits = new SearchHit[] { hit };
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3);
        adapter.adaptResponse(version, searchHits);
        assertNull(hit.getFields().get("obj"));

        adapter = createAdapter("obj.*", false);

        hit = new SearchHit(1).sourceRef(new BytesArray("{ \"obj\": { \"foo\" : \"value\"}}"));
        searchHits = new SearchHit[] { hit };
        adapter.adaptResponse(version, searchHits);
        assertNull(hit.getFields().get("obj"));
        assertEquals(1, hit.getFields().get("obj.foo").getValues().size());
        assertEquals("value", hit.getFields().get("obj.foo").getValues().get(0));
    }

    /**
     * test that on connections after 7.10, we don't modify the original search source
     */
    public void testAdaptMethodNoopBehaviour() {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchField("foo*").fetchSource(false);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(source);
        searchRequest.setFieldsOptionEmulationEnabled(true);
        FieldsOptionSourceAdapter adapter = new FieldsOptionSourceAdapter(searchRequest);

        Version connectionVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3);
        adapter.adaptRequest(connectionVersion, searchRequest::source);
        assertNotSame("Search source on request shouldn't be same after adaption", source, searchRequest.source());

        connectionVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_10_0, Version.CURRENT);
        searchRequest.source(source);
        adapter.adaptRequest(connectionVersion, searchRequest::source);
        assertSame("Search source on request should be same after adaption", source, searchRequest.source());
    }

    /**
     * when _source is enabled with includes/excludes on the original request, we error
     */
    public void testFieldsAdapterException() {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchField("foo*")
            .fetchSource(new FetchSourceContext(true, new String[] { "bar" }, new String[0]));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(source);
        searchRequest.setFieldsOptionEmulationEnabled(true);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> new FieldsOptionSourceAdapter(searchRequest));
        assertEquals(FieldsOptionSourceAdapter.FIELDS_EMULATION_ERROR_MSG, iae.getMessage());
    }

    private FieldsOptionSourceAdapter createAdapter(String fetchFields) {
        return createAdapter(fetchFields, true);
    }

    private FieldsOptionSourceAdapter createAdapter(String fetchFields, boolean fetchSource) {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchField(fetchFields).fetchSource(fetchSource);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(source);
        searchRequest.setFieldsOptionEmulationEnabled(true);
        return new FieldsOptionSourceAdapter(searchRequest);
    }
}
