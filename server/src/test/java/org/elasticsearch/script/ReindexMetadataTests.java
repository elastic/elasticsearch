/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;

public class ReindexMetadataTests extends ESTestCase {
    private static final String INDEX = "myIndex";
    private static final String ID = "myId";
    private static final long VERSION = 5;
    private static final String ROUTING = "myRouting";
    private static final String OP = "index";

    private static final long TIMESTAMP = 1_658_000_000_000L;
    private ReindexMetadata metadata;

    @Before
    public void initMetadata() throws Exception {
        reset();
    }

    protected void reset() {
        metadata = new ReindexMetadata(INDEX, ID, VERSION, ROUTING, OP, TIMESTAMP);
    }

    public void testIndex() {
        assertFalse(metadata.indexChanged());

        metadata.put("_index", INDEX);
        assertFalse(metadata.indexChanged());

        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> metadata.remove("_index"));
        assertEquals("_index cannot be removed", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> metadata.put("_index", null));
        assertEquals("_index cannot be null", err.getMessage());
        assertFalse(metadata.indexChanged());

        metadata.put("_index", "myIndex2");
        assertTrue(metadata.indexChanged());

        metadata.put("_index", INDEX);
        assertFalse(metadata.indexChanged());

        metadata.setIndex("myIndex3");
        assertTrue(metadata.indexChanged());
    }

    public void testId() {
        assertFalse(metadata.idChanged());

        metadata.put("_id", ID);
        assertFalse(metadata.idChanged());

        metadata.remove("_id");
        assertTrue(metadata.idChanged());
        assertNull(metadata.getId());

        metadata.put("_id", "myId2");
        assertTrue(metadata.idChanged());

        metadata.setId(ID);
        assertFalse(metadata.idChanged());

        metadata.setId("myId3");
        assertTrue(metadata.idChanged());
    }

    public void testRouting() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        assertFalse(metadata.routingChanged());

        metadata.put("_routing", ROUTING);
        assertFalse(metadata.routingChanged());
        assertEquals(ROUTING, metadata.get(SliceIndexing.PARAM_NAME));

        metadata.remove("_routing");
        assertTrue(metadata.routingChanged());
        assertNull(metadata.getRouting());
        assertNull(metadata.get(SliceIndexing.PARAM_NAME));

        metadata.put("_routing", "myRouting2");
        assertTrue(metadata.routingChanged());
        assertEquals("myRouting2", metadata.get(SliceIndexing.PARAM_NAME));

        metadata.setRouting(ROUTING);
        assertFalse(metadata.routingChanged());
        assertEquals(ROUTING, metadata.get(SliceIndexing.PARAM_NAME));

        metadata.setRouting("myRouting3");
        assertTrue(metadata.routingChanged());
        assertEquals("myRouting3", metadata.get(SliceIndexing.PARAM_NAME));
    }

    public void testRoutingAndSliceAliasMapStateStayConsistent() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        metadata.put("_routing", "routing1");
        assertTrue(metadata.containsKey("_routing"));
        assertTrue(metadata.containsKey(SliceIndexing.PARAM_NAME));
        assertTrue(metadata.containsValue("routing1"));
        assertFalse(metadata.isRoutingFromSlice());

        metadata.remove("_routing");
        assertFalse(metadata.containsKey("_routing"));
        assertFalse(metadata.containsKey(SliceIndexing.PARAM_NAME));
        assertFalse(metadata.containsValue("routing1"));

        metadata.put(SliceIndexing.PARAM_NAME, "slice1");
        assertTrue(metadata.containsKey("_routing"));
        assertTrue(metadata.containsKey(SliceIndexing.PARAM_NAME));
        assertTrue(metadata.containsValue("slice1"));
        assertTrue(metadata.isRoutingFromSlice());

        metadata.remove(SliceIndexing.PARAM_NAME);
        assertFalse(metadata.containsKey("_routing"));
        assertFalse(metadata.containsKey(SliceIndexing.PARAM_NAME));
        assertFalse(metadata.containsValue("slice1"));
    }

    public void testSliceAlias() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        assertFalse(metadata.routingChanged());

        metadata.put(SliceIndexing.PARAM_NAME, ROUTING);
        assertFalse(metadata.routingChanged());
        assertEquals(ROUTING, metadata.getRouting());

        metadata.remove(SliceIndexing.PARAM_NAME);
        assertTrue(metadata.routingChanged());
        assertNull(metadata.getRouting());

        metadata.put(SliceIndexing.PARAM_NAME, "slice2");
        assertTrue(metadata.routingChanged());
        assertEquals("slice2", metadata.getRouting());
        assertTrue(metadata.isRoutingFromSlice());
    }

    public void testRoutingProvenanceTracksSliceAliasUsage() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        metadata.put(SliceIndexing.PARAM_NAME, "slice2");
        assertTrue(metadata.routingChanged());
        assertTrue(metadata.isRoutingFromSlice());

        metadata.put("_routing", "routing2");
        assertTrue(metadata.routingChanged());
        assertFalse(metadata.isRoutingFromSlice());
    }

    public void testRoutingChangedWithSliceTracksProvenanceOnlyChanges() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        assertFalse(metadata.routingChangedWithSlice(false));
        assertTrue(metadata.routingChangedWithSlice(true));

        metadata.put(SliceIndexing.PARAM_NAME, ROUTING);
        assertFalse(metadata.routingChanged());
        assertTrue(metadata.isRoutingFromSlice());
        assertTrue(metadata.routingChangedWithSlice(false));
        assertFalse(metadata.routingChangedWithSlice(true));

        metadata.put("_routing", ROUTING);
        assertFalse(metadata.routingChanged());
        assertFalse(metadata.isRoutingFromSlice());
        assertFalse(metadata.routingChangedWithSlice(false));
        assertTrue(metadata.routingChangedWithSlice(true));
    }

    public void testRoutingChangedWithSliceTracksRoutingValueChanges() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        metadata.put("_routing", "routing2");
        assertTrue(metadata.routingChanged());
        assertTrue(metadata.routingChangedWithSlice(false));
        assertTrue(metadata.routingChangedWithSlice(true));
    }

    public void testSliceUnavailableWhenFeatureFlagDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        assertFalse(metadata.isAvailable(SliceIndexing.PARAM_NAME));
        assertFalse(metadata.keySet().contains(SliceIndexing.PARAM_NAME));
        assertNull(metadata.get(SliceIndexing.PARAM_NAME));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> metadata.put(SliceIndexing.PARAM_NAME, "slice1"));
        assertThat(e.getMessage(), containsString(SliceIndexing.PARAM_NAME + " cannot be updated"));
    }

    public void testVersion() {
        assertFalse(metadata.versionChanged());

        metadata.put("_version", VERSION);
        assertFalse(metadata.versionChanged());

        metadata.remove("_version");
        assertTrue(metadata.versionChanged());
        assertTrue(metadata.isVersionInternal());
        assertEquals(Long.MIN_VALUE, metadata.getVersion());
        assertNull(metadata.get("_version"));

        metadata.put("_version", VERSION + 5);
        assertTrue(metadata.versionChanged());

        metadata.setVersion(VERSION);
        assertFalse(metadata.versionChanged());

        metadata.setVersion(VERSION + 10);
        assertTrue(metadata.versionChanged());
        assertEquals(VERSION + 10, metadata.getVersion());

        ReindexMetadata.setVersionToInternal(metadata);
        assertTrue(metadata.isVersionInternal());
        assertEquals(Long.MIN_VALUE, metadata.getVersion());
        assertNull(metadata.get("_version"));
    }

    public void testOp() {
        assertEquals("index", metadata.getOp());
        assertEquals("index", metadata.get("op"));

        metadata.setOp("noop");
        assertEquals("noop", metadata.getOp());
        assertEquals("noop", metadata.get("op"));

        metadata.put("op", "delete");
        assertEquals("delete", metadata.getOp());

        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> metadata.setOp("bad"));
        assertEquals("[op] must be one of delete, index, noop, not [bad]", err.getMessage());

        err = expectThrows(IllegalArgumentException.class, () -> metadata.put("op", "malo"));
        assertEquals("[op] must be one of delete, index, noop, not [malo]", err.getMessage());
    }
}
