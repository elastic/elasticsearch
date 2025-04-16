/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fieldcaps;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesBuilder;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class FieldCapsHasValueTests extends ESIntegTestCase {
    private final String INDEX1 = "index-1";
    private final String ALIAS1 = "alias-1";
    private final String INDEX2 = "index-2";
    private final String INDEX3 = "index-3";

    @Before
    public void setUpIndices() {
        assertAcked(
            prepareCreate(INDEX1).setWaitForActiveShards(ActiveShardCount.ALL)
                .setSettings(indexSettings())
                .setMapping("foo", "type=text", "bar", "type=keyword", "bar-alias", "type=alias,path=bar", "timestamp", "type=date")
        );
        assertAcked(
            prepareCreate(INDEX2).setWaitForActiveShards(ActiveShardCount.ALL)
                .setSettings(indexSettings())
                .setMapping("bar", "type=date", "timestamp", "type=date")
        );
        assertAcked(
            prepareCreate(INDEX3).setWaitForActiveShards(ActiveShardCount.ALL)
                .setSettings(indexSettings())
                .setMapping("nested_type", "type=nested", "object.sub_field", "type=keyword,store=true")
        );
        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias(INDEX1, ALIAS1));
    }

    public void testNoFieldsInEmptyIndex() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1, INDEX2, INDEX3);
        // Ensure the response has no mapped fields.
        assertFalse(response.get().containsKey("foo"));
        assertFalse(response.get().containsKey("bar"));
        assertFalse(response.get().containsKey("bar-alias"));
    }

    public void testOnlyFieldsWithValueInIndex() {
        prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        refresh(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1, INDEX2, INDEX3);
        assertThat(response.get(), Matchers.hasKey("foo"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertThat(fooField, Matchers.hasKey("text"));
        assertEquals(new FieldCapabilitiesBuilder("foo", "text").isAggregatable(false).build(), fooField.get("text"));
    }

    public void testOnlyFieldsWithValueInAlias() {
        prepareIndex(ALIAS1).setSource("foo", "foo-text").get();
        refresh(ALIAS1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1, INDEX2, INDEX3);
        assertThat(response.get(), Matchers.hasKey("foo"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertThat(fooField, Matchers.hasKey("text"));
        assertEquals(new FieldCapabilitiesBuilder("foo", "text").isAggregatable(false).build(), fooField.get("text"));
    }

    public void testOnlyFieldsWithValueInSpecifiedIndex() {
        prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        refresh(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX1).setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1);
        assertThat(response.get(), Matchers.hasKey("foo"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertThat(fooField, Matchers.hasKey("text"));
        assertEquals(new FieldCapabilitiesBuilder("foo", "text").isAggregatable(false).build(), fooField.get("text"));
    }

    public void testOnlyFieldsWithValueInSpecifiedAlias() {
        prepareIndex(ALIAS1).setSource("foo", "foo-text").get();
        refresh(ALIAS1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps(ALIAS1).setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1);
        assertThat(response.get(), Matchers.hasKey("foo"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertThat(fooField, Matchers.hasKey("text"));
        assertEquals(new FieldCapabilitiesBuilder("foo", "text").isAggregatable(false).build(), fooField.get("text"));
    }

    public void testFieldsWithValueAfterUpdate() {
        DocWriteResponse doc = prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        prepareIndex(INDEX1).setId(doc.getId()).setSource("bar", "bar-keyword").get();
        refresh(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX1).setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1);
        assertThat(response.get(), Matchers.hasKey("foo"));
        assertThat(response.get(), Matchers.hasKey("bar"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertThat(fooField, Matchers.hasKey("text"));
        assertEquals(new FieldCapabilitiesBuilder("foo", "text").isAggregatable(false).build(), fooField.get("text"));
        // Check the capabilities for the 'bar' field.
        Map<String, FieldCapabilities> barField = response.getField("bar");
        assertEquals(1, barField.size());
        assertThat(barField, Matchers.hasKey("keyword"));
        assertEquals(new FieldCapabilitiesBuilder("bar", "keyword").build(), barField.get("keyword"));
    }

    public void testOnlyFieldsWithValueAfterNodesRestart() throws Exception {
        prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        internalCluster().fullRestart();
        ensureGreen(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX1).setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1);
        assertThat(response.get(), Matchers.hasKey("foo"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertThat(fooField, Matchers.hasKey("text"));
        assertEquals(new FieldCapabilitiesBuilder("foo", "text").isAggregatable(false).build(), fooField.get("text"));
    }

    public void testFieldsAndAliasWithValue() {
        prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        prepareIndex(INDEX1).setSource("bar", "bar-keyword").get();
        refresh(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1, INDEX2, INDEX3);
        assertThat(response.get(), Matchers.hasKey("foo"));
        assertThat(response.get(), Matchers.hasKey("bar"));
        assertThat(response.get(), Matchers.hasKey("bar-alias"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertThat(fooField, Matchers.hasKey("text"));
        assertEquals(new FieldCapabilitiesBuilder("foo", "text").isAggregatable(false).build(), fooField.get("text"));
        // Check the capabilities for the 'bar' field.
        Map<String, FieldCapabilities> barField = response.getField("bar");
        assertEquals(1, barField.size());
        assertThat(barField, Matchers.hasKey("keyword"));
        assertEquals(new FieldCapabilitiesBuilder("bar", "keyword").build(), barField.get("keyword"));
        // Check the capabilities for the 'bar-alias' field.
        Map<String, FieldCapabilities> barAlias = response.getField("bar-alias");
        assertEquals(1, barAlias.size());
        assertThat(barAlias, Matchers.hasKey("keyword"));
        assertEquals(new FieldCapabilitiesBuilder("bar-alias", "keyword").build(), barAlias.get("keyword"));
    }

    public void testUnmappedFieldsWithValueAfterRestart() throws Exception {
        prepareIndex(INDEX1).setSource("unmapped", "unmapped-text").get();
        internalCluster().fullRestart();
        ensureGreen(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps()
            .setFields("*")
            .setIncludeUnmapped(true)
            .setincludeEmptyFields(false)
            .get();

        assertIndices(response, INDEX1, INDEX2, INDEX3);
        assertThat(response.get(), Matchers.hasKey("unmapped"));
        // Check the capabilities for the 'unmapped' field.
        Map<String, FieldCapabilities> unmappedField = response.getField("unmapped");
        assertEquals(2, unmappedField.size());
        assertThat(unmappedField, Matchers.hasKey("text"));
        assertEquals(
            new FieldCapabilitiesBuilder("unmapped", "text").isAggregatable(false).indices(INDEX1).build(),
            unmappedField.get("text")
        );
    }

    public void testTwoFieldsNameTwoIndices() {
        prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        prepareIndex(INDEX2).setSource("bar", 1704293160000L).get();
        refresh(INDEX1, INDEX2);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1, INDEX2, INDEX3);
        assertThat(response.get(), Matchers.hasKey("foo"));
        assertThat(response.get(), Matchers.hasKey("bar"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertThat(fooField, Matchers.hasKey("text"));
        assertEquals(new FieldCapabilitiesBuilder("foo", "text").isAggregatable(false).build(), fooField.get("text"));
        // Check the capabilities for the 'bar' field.
        Map<String, FieldCapabilities> barField = response.getField("bar");
        assertEquals(1, barField.size());
        assertThat(barField, Matchers.hasKey("date"));
        assertEquals(new FieldCapabilitiesBuilder("bar", "date").build(), barField.get("date"));
    }

    public void testSameFieldNameTwoIndices() {
        prepareIndex(INDEX1).setSource("bar", "bar-text").get();
        prepareIndex(INDEX2).setSource("bar", 1704293160000L).get();
        refresh(INDEX1, INDEX2);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1, INDEX2, INDEX3);
        assertThat(response.get(), Matchers.hasKey("bar"));
        // Check the capabilities for the 'bar' field.
        Map<String, FieldCapabilities> barField = response.getField("bar");
        assertEquals(2, barField.size());
        assertThat(barField, Matchers.hasKey("keyword"));
        assertEquals(new FieldCapabilitiesBuilder("bar", "keyword").indices(INDEX1).build(), barField.get("keyword"));
        assertThat(barField, Matchers.hasKey("date"));
        assertEquals(new FieldCapabilitiesBuilder("bar", "date").indices(INDEX2).build(), barField.get("date"));
    }

    public void testDeletedDocsReturned() {
        // In this current implementation we do not handle deleted documents (without a restart).
        // This test should fail if in a future implementation we handle deletes.
        DocWriteResponse foo = prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        client().prepareDelete().setIndex(INDEX1).setId(foo.getId()).get();
        client().admin().indices().prepareForceMerge(INDEX1).setFlush(true).setMaxNumSegments(1).get();
        refresh(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX1, INDEX2, INDEX3);
        assertThat(response.get(), Matchers.hasKey("foo"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertThat(fooField, Matchers.hasKey("text"));
        assertEquals(new FieldCapabilitiesBuilder("foo", "text").isAggregatable(false).build(), fooField.get("text"));
    }

    public void testNoNestedFieldsInEmptyIndex() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX3).setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX3);
        assertFalse(response.get().containsKey("nested_type"));
        assertFalse(response.get().containsKey("nested_type.nested_field"));
    }

    public void testNestedFields() {
        prepareIndex(INDEX3).setSource("nested_type", Collections.singletonMap("nested_field", "value")).get();
        refresh(INDEX3);

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX3).setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX3);
        assertThat(response.get(), Matchers.hasKey("nested_type"));
        assertThat(response.get(), Matchers.hasKey("nested_type.nested_field"));
        // Check the capabilities for the 'nested_type' field.
        Map<String, FieldCapabilities> nestedTypeField = response.getField("nested_type");
        assertEquals(1, nestedTypeField.size());
        assertThat(nestedTypeField, Matchers.hasKey("nested"));
        assertEquals(
            new FieldCapabilitiesBuilder("nested_type", "nested").isSearchable(false).isAggregatable(false).build(),
            nestedTypeField.get("nested")
        );
        // Check the capabilities for the 'nested_type.nested_field' field.
        Map<String, FieldCapabilities> nestedTypeNestedField = response.getField("nested_type.nested_field");
        assertEquals(1, nestedTypeNestedField.size());
        assertThat(nestedTypeNestedField, Matchers.hasKey("text"));
        assertEquals(
            new FieldCapabilitiesBuilder("nested_type.nested_field", "text").isAggregatable(false).build(),
            nestedTypeNestedField.get("text")
        );
    }

    public void testNoObjectFieldsInEmptyIndex() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX3).setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX3);
        assertFalse(response.get().containsKey("object"));
        assertFalse(response.get().containsKey("object.sub_field"));
    }

    public void testObjectFields() {
        prepareIndex(INDEX3).setSource("object.sub_field", "value").get();
        refresh(INDEX3);

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX3).setFields("*").setincludeEmptyFields(false).get();

        assertIndices(response, INDEX3);
        assertThat(response.get(), Matchers.hasKey("object"));
        assertThat(response.get(), Matchers.hasKey("object.sub_field"));
        // Check the capabilities for the 'object' field.
        Map<String, FieldCapabilities> objectTypeField = response.getField("object");
        assertEquals(1, objectTypeField.size());
        assertThat(objectTypeField, Matchers.hasKey("object"));
        assertEquals(
            new FieldCapabilitiesBuilder("object", "object").isSearchable(false).isAggregatable(false).build(),
            objectTypeField.get("object")
        );
        // Check the capabilities for the 'object.sub_field' field.
        Map<String, FieldCapabilities> objectSubfield = response.getField("object.sub_field");
        assertEquals(1, objectSubfield.size());
        assertThat(objectSubfield, Matchers.hasKey("keyword"));
        assertEquals(new FieldCapabilitiesBuilder("object.sub_field", "keyword").build(), objectSubfield.get("keyword"));
    }

    public void testWithIndexFilter() throws InterruptedException {

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(prepareIndex(INDEX1).setSource("timestamp", "2015-07-08"));
        reqs.add(prepareIndex(INDEX1).setSource("timestamp", "2018-07-08"));
        reqs.add(prepareIndex(INDEX2).setSource("timestamp", "2019-10-12"));
        reqs.add(prepareIndex(INDEX2).setSource("timestamp", "2020-07-08"));
        indexRandom(true, reqs);

        FieldCapabilitiesResponse response = client().prepareFieldCaps("index-*")
            .setFields("*")
            .setIndexFilter(QueryBuilders.rangeQuery("timestamp").gte("2019-11-01"))
            .setincludeEmptyFields(false)
            .get();
        assertIndices(response, INDEX2);
        // Check the capabilities for the 'timestamp' field.
        Map<String, FieldCapabilities> timestampField = response.getField("timestamp");
        assertEquals(1, timestampField.size());
        assertThat(timestampField, Matchers.hasKey("date"));
        assertNull(response.getField("foo"));
        assertNull(response.getField("bar"));
        assertNull(response.getField("bar"));

        response = client().prepareFieldCaps("index-*")
            .setFields("*")
            .setIndexFilter(QueryBuilders.rangeQuery("timestamp").lte("2017-01-01"))
            .setincludeEmptyFields(false)
            .get();
        assertIndices(response, INDEX1);
        // Check the capabilities for the 'timestamp' field.
        timestampField = response.getField("timestamp");
        assertEquals(1, timestampField.size());
        assertThat(timestampField, Matchers.hasKey("date"));
        assertNull(response.getField("foo"));
        assertNull(response.getField("bar"));
    }

    private void assertIndices(FieldCapabilitiesResponse response, String... indices) {
        assertNotNull(response.getIndices());
        Arrays.sort(indices);
        Arrays.sort(response.getIndices());
        assertArrayEquals(indices, response.getIndices());
    }

}
