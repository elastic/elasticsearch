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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.transport.Transport;

public class FieldsOptionSourceAdapterTests extends ESTestCase {

    /**
     * test that on versions after 7.10 or when no fields are fetched, the returned adapter does nothing (is the noop adapter)
     */
    public void testNoopFieldsAdapterCreation() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        FieldsOptionSourceAdapter adapter = FieldsOptionSourceAdapter.create(
            mockConnection(VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3)),
            source
        );
        assertSame(FieldsOptionSourceAdapter.NOOP_ADAPTER, adapter);

        adapter = FieldsOptionSourceAdapter.create(
            mockConnection(VersionUtils.randomVersionBetween(random(), Version.V_7_10_0, Version.CURRENT)),
            source
        );
        // no set fields should also return a noop adapter
        assertSame(FieldsOptionSourceAdapter.NOOP_ADAPTER, adapter);
    }

    /**
     * test that with set fields pattern, the adapter does something.
     * check correct request and response modifications
     */
    public void testFieldsAdapterNoSource() {
        // build adapter that gets all foo* fields
        SearchSourceBuilder source = new SearchSourceBuilder().fetchField("foo*");
        FieldsOptionSourceAdapter adapter = FieldsOptionSourceAdapter.create(
            mockConnection(VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3)),
            source
        );

        SetOnce<SearchSourceBuilder> rewrittenSource = new SetOnce<>();
        adapter.adaptRequest(rewrittenSource::set);
        assertNull(rewrittenSource.get().fetchSource());
        assertFalse(adapter.getRemoveSourceOnResponse());

        SearchHit hit = new SearchHit(1).sourceRef(new BytesArray("{ \"foo\" : \"bar\", \"fuzz\":2, \"foo.bar\":[4,5]}"));
        SearchHit[] searchHits = new SearchHit[] { hit };
        assertNull(hit.getFields().get("foo"));
        assertNull(hit.getFields().get("foo.bar"));
        assertNull(hit.getFields().get("fuzz"));
        adapter.adaptResponse(searchHits);
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
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(false).fetchField("foo*");
        FieldsOptionSourceAdapter adapter = FieldsOptionSourceAdapter.create(
            mockConnection(VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3)),
            source
        );
        SetOnce<SearchSourceBuilder> rewrittenSource = new SetOnce<>();
        adapter.adaptRequest(rewrittenSource::set);
        assertNotNull(rewrittenSource.get().fetchSource());
        assertTrue(rewrittenSource.get().fetchSource().fetchSource());
        assertEquals(1, rewrittenSource.get().fetchSource().includes().length);
        assertEquals("foo*", rewrittenSource.get().fetchSource().includes()[0]);
        assertTrue(adapter.getRemoveSourceOnResponse());

        SearchHit hit = new SearchHit(1).sourceRef(new BytesArray("{ \"foo\" : \"bar\", \"fuzz\":2, \"foo.bar\":[4,5]}"));
        SearchHit[] searchHits = new SearchHit[] { hit };
        adapter.adaptResponse(searchHits);
        assertFalse(hit.hasSource());
    }

    public void testFieldsAdapterSourceTrue() {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(true).fetchField("foo*");
        FieldsOptionSourceAdapter adapter = FieldsOptionSourceAdapter.create(
            mockConnection(VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3)),
            source
        );
        SetOnce<SearchSourceBuilder> rewrittenSource = new SetOnce<>();
        adapter.adaptRequest(rewrittenSource::set);
        assertSame(source, rewrittenSource.get());
        assertFalse(adapter.getRemoveSourceOnResponse());

        SearchHit hit = new SearchHit(1).sourceRef(new BytesArray("{ \"foo\" : \"bar\", \"fuzz\":2, \"foo.bar\":[4,5]}"));
        SearchHit[] searchHits = new SearchHit[] { hit };
        adapter.adaptResponse(searchHits);
        assertTrue(hit.hasSource());
    }

    /**
     * while querying the root or object fields works in "_source" filtering, we should not
     * return this via the fields API, since our regular lookup there only returns flattened leaf values
     */
    public void testFieldsAdapterObjects() {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(false).fetchField("obj");
        FieldsOptionSourceAdapter adapter = FieldsOptionSourceAdapter.create(
            mockConnection(VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3)),
            source
        );

        SearchHit hit = new SearchHit(1).sourceRef(new BytesArray("{ \"obj\": { \"foo\" : \"value\"}}"));
        SearchHit[] searchHits = new SearchHit[] { hit };
        adapter.adaptResponse(searchHits);
        assertNull(hit.getFields().get("obj"));

        source = new SearchSourceBuilder().fetchSource(false).fetchField("obj.*");
        adapter = FieldsOptionSourceAdapter.create(
            mockConnection(VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3)),
            source
        );

        hit = new SearchHit(1).sourceRef(new BytesArray("{ \"obj\": { \"foo\" : \"value\"}}"));
        searchHits = new SearchHit[] { hit };
        adapter.adaptResponse(searchHits);
        assertNull(hit.getFields().get("obj"));
        assertEquals(1, hit.getFields().get("obj.foo").getValues().size());
        assertEquals("value", hit.getFields().get("obj.foo").getValues().get(0));
    }

    /**
     * when _source is enabled with includes/excludes on the original request, we error
     */
    public void testFieldsAdapterException() {
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> FieldsOptionSourceAdapter.create(
            mockConnection(VersionUtils.randomVersionBetween(random(), Version.V_6_8_18, Version.V_7_9_3)),
            new SearchSourceBuilder().fetchField("foo*").fetchSource(new FetchSourceContext(true, new String[] { "bar" }, new String[0]))
        ));
        assertEquals(FieldsOptionSourceAdapter.FIELDS_EMULATION_ERROR_MSG, iae.getMessage());
    }

    private Transport.Connection mockConnection(Version version) {
        DiscoveryNode node = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), version);
        return new SearchAsyncActionTests.MockConnection(node);
    }
}
