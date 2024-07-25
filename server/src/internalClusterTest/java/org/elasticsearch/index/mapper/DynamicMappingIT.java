/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.field.WriteField;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;

public class DynamicMappingIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testConflictingDynamicMappings() {
        // we don't use indexRandom because the order of requests is important here
        createIndex("index");
        prepareIndex("index").setId("1").setSource("foo", 3).get();
        try {
            prepareIndex("index").setId("2").setSource("foo", "bar").get();
            fail("Indexing request should have failed!");
        } catch (DocumentParsingException e) {
            // general case, the parsing code complains that it can't parse "bar" as a "long"
            assertThat(e.getMessage(), Matchers.containsString("failed to parse field [foo] of type [long]"));
        } catch (IllegalArgumentException e) {
            // rare case: the node that processes the index request doesn't have the mappings
            // yet and sends a mapping update to the master node to map "bar" as "text". This
            // fails as it had been already mapped as a long by the previous index request.
            assertThat(e.getMessage(), Matchers.containsString("mapper [foo] cannot be changed from type [long] to [text]"));
        }
    }

    public void testSimpleDynamicMappingsSuccessful() {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("a.x", 1).get();
        client().prepareIndex("index").setId("2").setSource("a.y", 2).get();

        Map<String, Object> mappings = indicesAdmin().prepareGetMappings("index").get().mappings().get("index").sourceAsMap();
        assertTrue(new WriteField("properties.a", () -> mappings).exists());
        assertTrue(new WriteField("properties.a.properties.x", () -> mappings).exists());
    }

    public void testConflictingDynamicMappingsBulk() {
        // we don't use indexRandom because the order of requests is important here
        createIndex("index");
        prepareIndex("index").setId("1").setSource("foo", 3).get();
        BulkResponse bulkResponse = client().prepareBulk().add(prepareIndex("index").setId("1").setSource("foo", 3)).get();
        assertFalse(bulkResponse.hasFailures());
        bulkResponse = client().prepareBulk().add(prepareIndex("index").setId("2").setSource("foo", "bar")).get();
        assertTrue(bulkResponse.hasFailures());
    }

    public void testArrayWithDifferentTypes() {
        createIndex("index");
        BulkResponse bulkResponse = client().prepareBulk()
            .add(client().prepareIndex("index").setId("1").setSource("foo", List.of(42, "bar")))
            .get();

        assertTrue(bulkResponse.hasFailures());
        assertEquals(
            "mapper [foo] cannot be changed from type [long] to [text]",
            bulkResponse.getItems()[0].getFailure().getCause().getMessage()
        );
    }

    public void testArraysCountAsOneTowardsFieldLimit() {
        createIndex("index", Settings.builder().put(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 2).build());
        BulkResponse bulkResponse = client().prepareBulk()
            .add(client().prepareIndex("index").setId("1").setSource("field1", List.of(1, 2), "field2", 1))
            .get();

        assertFalse(bulkResponse.hasFailures());
    }

    public void testConcurrentDynamicUpdates() throws Throwable {
        int numberOfFieldsToCreate = 32;
        Map<String, Object> properties = indexConcurrently(numberOfFieldsToCreate, Settings.builder());
        assertThat(properties, aMapWithSize(numberOfFieldsToCreate));
        for (int i = 0; i < numberOfFieldsToCreate; i++) {
            assertThat(properties, hasKey("field" + i));
        }
    }

    public void testConcurrentDynamicIgnoreBeyondLimitUpdates() throws Throwable {
        int numberOfFieldsToCreate = 32;
        Map<String, Object> properties = indexConcurrently(
            numberOfFieldsToCreate,
            Settings.builder()
                .put(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), numberOfFieldsToCreate)
                .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
        );
        // every field is a multi-field (text + keyword)
        assertThat(properties, aMapWithSize(16));
        assertResponse(
            prepareSearch("index").setQuery(new MatchAllQueryBuilder()).setSize(numberOfFieldsToCreate).addFetchField("*"),
            response -> {
                long ignoredFields = Arrays.stream(response.getHits().getHits()).filter(hit -> hit.field("_ignored") != null).count();
                assertEquals(16, ignoredFields);
            }
        );
    }

    private Map<String, Object> indexConcurrently(int numberOfFieldsToCreate, Settings.Builder settings) throws Throwable {
        indicesAdmin().prepareCreate("index").setSettings(settings).get();
        ensureGreen("index");
        final AtomicReference<Throwable> error = new AtomicReference<>();
        startInParallel(numberOfFieldsToCreate, i -> {
            final String id = Integer.toString(i);
            try {
                assertEquals(
                    DocWriteResponse.Result.CREATED,
                    prepareIndex("index").setId(id).setSource("field" + id, "bar").get().getResult()
                );
            } catch (Exception e) {
                error.compareAndSet(null, e);
            }
        });
        if (error.get() != null) {
            throw error.get();
        }
        client().admin().indices().prepareRefresh("index").get();
        for (int i = 0; i < numberOfFieldsToCreate; ++i) {
            assertTrue(client().prepareGet("index", Integer.toString(i)).get().isExists());
        }
        GetMappingsResponse mappings = indicesAdmin().prepareGetMappings("index").get();
        MappingMetadata indexMappings = mappings.getMappings().get("index");
        assertNotNull(indexMappings);
        Map<String, Object> typeMappingsMap = indexMappings.getSourceAsMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) typeMappingsMap.get("properties");
        return properties;
    }

    public void testPreflightCheckAvoidsMaster() throws InterruptedException, IOException {
        // can't use INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING nor INDEX_MAPPING_DEPTH_LIMIT_SETTING as a check here, as that is already
        // checked at parse time, see testTotalFieldsLimitForDynamicMappingsUpdateCheckedAtDocumentParseTime
        createIndex("index", Settings.builder().put(INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 2).build());
        ensureGreen("index");
        indicesAdmin().preparePutMapping("index")
            .setSource(
                Strings.toString(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("dynamic_templates")
                        .startObject()
                        .startObject("test")
                        .field("match", "nested*")
                        .startObject("mapping")
                        .field("type", "nested")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
            .get();
        prepareIndex("index").setId("1").setSource("nested1", Map.of("foo", "bar"), "nested2", Map.of("foo", "bar")).get();

        final CountDownLatch masterBlockedLatch = new CountDownLatch(1);
        final CountDownLatch indexingCompletedLatch = new CountDownLatch(1);

        internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName())
            .submitUnbatchedStateUpdateTask("block-state-updates", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    masterBlockedLatch.countDown();
                    indexingCompletedLatch.await();
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });

        masterBlockedLatch.await();
        try {
            assertThat(
                expectThrows(IllegalArgumentException.class, prepareIndex("index").setId("2").setSource("nested3", Map.of("foo", "bar")))
                    .getMessage(),
                Matchers.containsString("Limit of nested fields [2] has been exceeded")
            );
        } finally {
            indexingCompletedLatch.countDown();
        }
    }

    public void testTotalFieldsLimitForDynamicMappingsUpdateCheckedAtDocumentParseTime() throws InterruptedException {
        createIndex("index", Settings.builder().put(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 2).build());
        ensureGreen("index");
        prepareIndex("index").setId("1").setSource("field1", "value1").get();

        final CountDownLatch masterBlockedLatch = new CountDownLatch(1);
        final CountDownLatch indexingCompletedLatch = new CountDownLatch(1);

        internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName())
            .submitUnbatchedStateUpdateTask("block-state-updates", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    masterBlockedLatch.countDown();
                    indexingCompletedLatch.await();
                    return currentState;
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });

        masterBlockedLatch.await();
        try {
            Exception e = expectThrows(DocumentParsingException.class, prepareIndex("index").setId("2").setSource("field2", "value2"));
            assertThat(e.getMessage(), Matchers.containsString("failed to parse"));
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(e.getCause().getMessage(), Matchers.containsString("Limit of total fields [2] has been exceeded"));
        } finally {
            indexingCompletedLatch.countDown();
        }
    }

    public void testIgnoreDynamicBeyondLimitSingleMultiField() {
        indexIgnoreDynamicBeyond(1, orderedMap("field1", "text"), fields -> {
            assertThat(fields.keySet(), equalTo(Set.of("_ignored")));
            assertThat(fields.get("_ignored").getValues(), equalTo(List.of("field1")));
        });
    }

    public void testIgnoreDynamicBeyondLimitMultiField() {
        indexIgnoreDynamicBeyond(2, orderedMap("field1", 1, "field2", "text"), fields -> {
            assertThat(fields.keySet(), equalTo(Set.of("field1", "_ignored")));
            assertThat(fields.get("field1").getValues(), equalTo(List.of(1L)));
            assertThat(fields.get("_ignored").getValues(), equalTo(List.of("field2")));
        });
    }

    public void testIgnoreDynamicArrayField() {
        indexIgnoreDynamicBeyond(1, orderedMap("field1", 1, "field2", List.of(1, 2)), fields -> {
            assertThat(fields.keySet(), equalTo(Set.of("field1", "_ignored")));
            assertThat(fields.get("field1").getValues(), equalTo(List.of(1L)));
            assertThat(fields.get("_ignored").getValues(), equalTo(List.of("field2")));
        });
    }

    public void testIgnoreDynamicBeyondLimitObjectField() {
        indexIgnoreDynamicBeyond(3, orderedMap("a.b", 1, "a.c", 2, "a.d", 3), fields -> {
            assertThat(fields.keySet(), equalTo(Set.of("a.b", "a.c", "_ignored")));
            assertThat(fields.get("a.b").getValues(), equalTo(List.of(1L)));
            assertThat(fields.get("a.c").getValues(), equalTo(List.of(2L)));
            assertThat(fields.get("_ignored").getValues(), equalTo(List.of("a.d")));
        });
    }

    public void testIgnoreDynamicBeyondLimitObjectField2() {
        indexIgnoreDynamicBeyond(3, orderedMap("a.b", 1, "a.c", 2, "b.a", 3), fields -> {
            assertThat(fields.keySet(), equalTo(Set.of("a.b", "a.c", "_ignored")));
            assertThat(fields.get("a.b").getValues(), equalTo(List.of(1L)));
            assertThat(fields.get("a.c").getValues(), equalTo(List.of(2L)));
            assertThat(fields.get("_ignored").getValues(), equalTo(List.of("b")));
        });
    }

    public void testIgnoreDynamicBeyondLimitDottedObjectMultiField() {
        indexIgnoreDynamicBeyond(4, orderedMap("a.b", "foo", "a.c", 2, "a.d", 3), fields -> {
            assertThat(fields.keySet(), equalTo(Set.of("a.b", "a.b.keyword", "a.c", "_ignored")));
            assertThat(fields.get("a.b").getValues(), equalTo(List.of("foo")));
            assertThat(fields.get("a.b.keyword").getValues(), equalTo(List.of("foo")));
            assertThat(fields.get("a.c").getValues(), equalTo(List.of(2L)));
            assertThat(fields.get("_ignored").getValues(), equalTo(List.of("a.d")));
        });
    }

    public void testIgnoreDynamicBeyondLimitObjectMultiField() {
        indexIgnoreDynamicBeyond(5, orderedMap("a", orderedMap("b", "foo", "c", "bar", "d", 3)), fields -> {
            assertThat(fields.keySet(), equalTo(Set.of("a.b", "a.b.keyword", "a.c", "a.c.keyword", "_ignored")));
            assertThat(fields.get("a.b").getValues(), equalTo(List.of("foo")));
            assertThat(fields.get("a.b.keyword").getValues(), equalTo(List.of("foo")));
            assertThat(fields.get("a.c").getValues(), equalTo(List.of("bar")));
            assertThat(fields.get("a.c.keyword").getValues(), equalTo(List.of("bar")));
            assertThat(fields.get("_ignored").getValues(), equalTo(List.of("a.d")));
        });
    }

    public void testIgnoreDynamicBeyondLimitRuntimeFields() {
        indexIgnoreDynamicBeyond(1, orderedMap("field1", 1, "field2", List.of(1, 2)), Map.of("dynamic", "runtime"), fields -> {
            assertThat(fields.keySet(), equalTo(Set.of("field1", "_ignored")));
            assertThat(fields.get("field1").getValues(), equalTo(List.of(1L)));
            assertThat(fields.get("_ignored").getValues(), equalTo(List.of("field2")));
        });
    }

    public void testFieldLimitRuntimeAndDynamic() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(
                    Settings.builder()
                        .put(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 5)
                        .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
                )
                .setMapping("""
                    {
                      "dynamic": "runtime",
                      "properties": {
                        "runtime": {
                          "type": "object"
                        },
                        "mapped_obj": {
                          "type": "object",
                          "dynamic": "true"
                        }
                      }
                    }""")
                .get()
        );

        client().index(
            new IndexRequest("test").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(orderedMap("dynamic.keyword", "foo", "mapped_obj.number", 1, "mapped_obj.string", "foo"))
        ).get();

        assertResponse(prepareSearch("test").setQuery(new MatchAllQueryBuilder()).addFetchField("*"), r -> {
            var fields = r.getHits().getHits()[0].getFields();
            assertThat(fields.keySet(), equalTo(Set.of("dynamic.keyword", "mapped_obj.number", "_ignored")));
            assertThat(fields.get("dynamic.keyword").getValues(), equalTo(List.of("foo")));
            assertThat(fields.get("mapped_obj.number").getValues(), equalTo(List.of(1L)));
            assertThat(fields.get("_ignored").getValues(), equalTo(List.of("mapped_obj.string")));
        });
    }

    private LinkedHashMap<String, Object> orderedMap(Object... entries) {
        var map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put((String) entries[i], entries[i + 1]);
        }
        return map;
    }

    private void indexIgnoreDynamicBeyond(int fieldLimit, Map<String, Object> source, Consumer<Map<String, DocumentField>> fieldsConsumer) {
        indexIgnoreDynamicBeyond(fieldLimit, source, Map.of(), fieldsConsumer);
    }

    private void indexIgnoreDynamicBeyond(
        int fieldLimit,
        Map<String, Object> source,
        Map<String, Object> mapping,
        Consumer<Map<String, DocumentField>> fieldsConsumer
    ) {
        client().admin()
            .indices()
            .prepareCreate("index")
            .setSettings(
                Settings.builder()
                    .put(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), fieldLimit)
                    .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
                    .build()
            )
            .setMapping(mapping)
            .get();
        ensureGreen("index");
        client().prepareIndex("index").setId("1").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).setSource(source).get();
        assertResponse(
            prepareSearch("index").setQuery(new MatchAllQueryBuilder()).addFetchField("*"),
            r -> fieldsConsumer.accept(r.getHits().getHits()[0].getFields())
        );
    }

    public void testTotalFieldsLimitWithRuntimeFields() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 4)
            .build();

        String mapping = """
                {
                  "dynamic":"runtime",
                  "runtime": {
                    "my_object.rfield1": {
                       "type": "keyword"
                    },
                    "rfield2": {
                      "type": "keyword"
                    }
                  },
                  "properties": {
                    "field3" : {
                      "type": "keyword"
                    }
                  }
                }
            """;

        indicesAdmin().prepareCreate("index1").setSettings(indexSettings).setMapping(mapping).get();
        ensureGreen("index1");

        {
            // introduction of a new object with 2 new sub-fields fails
            Exception exc = expectThrows(
                DocumentParsingException.class,
                prepareIndex("index1").setId("1")
                    .setSource("field3", "value3", "my_object2", Map.of("new_field1", "value1", "new_field2", "value2"))
            );
            assertThat(exc.getMessage(), Matchers.containsString("failed to parse"));
            assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(
                exc.getCause().getMessage(),
                Matchers.containsString("Limit of total fields [4] has been exceeded while adding new fields [2]")
            );
        }

        {
            // introduction of a new single field succeeds
            prepareIndex("index1").setId("2").setSource("field3", "value3", "new_field4", 100).get();
        }

        {
            // remove 2 runtime field mappings
            assertAcked(indicesAdmin().preparePutMapping("index1").setSource("""
                    {
                      "runtime": {
                        "my_object.rfield1": null,
                        "rfield2" : null
                      }
                    }
                """, XContentType.JSON));

            // introduction of a new object with 2 new sub-fields succeeds
            prepareIndex("index1").setId("1")
                .setSource("field3", "value3", "my_object2", Map.of("new_field1", "value1", "new_field2", "value2"));
        }
    }

    public void testMappingVersionAfterDynamicMappingUpdate() throws Exception {
        createIndex("test");
        final ClusterService clusterService = internalCluster().clusterService();
        final long previousVersion = clusterService.state().metadata().index("test").getMappingVersion();
        prepareIndex("test").setId("1").setSource("field", "text").get();
        assertBusy(() -> assertThat(clusterService.state().metadata().index("test").getMappingVersion(), equalTo(1 + previousVersion)));
    }

    public void testBulkRequestWithDynamicTemplates() throws Exception {
        final XContentBuilder mappings = XContentFactory.jsonBuilder();
        mappings.startObject();
        {
            mappings.startArray("dynamic_templates");
            {
                mappings.startObject();
                mappings.startObject("location");
                {
                    if (randomBoolean()) {
                        mappings.field("match", "location");
                    }
                    if (randomBoolean()) {
                        mappings.field("match_mapping_type", "string");
                    }
                    mappings.startObject("mapping");
                    {
                        mappings.field("type", "geo_point");
                    }
                    mappings.endObject();
                }
                mappings.endObject();
                mappings.endObject();
            }
            mappings.endArray();
        }
        mappings.endObject();
        assertAcked(indicesAdmin().prepareCreate("test").setMapping(mappings));
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(
            new IndexRequest("test").id("1").source("location", "41.12,-71.34").setDynamicTemplates(Map.of("location", "location"))
        );
        requests.add(
            new IndexRequest("test").id("2")
                .source(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("location")
                        .field("lat", 41.12)
                        .field("lon", -71.34)
                        .endObject()
                        .endObject()
                )
                .setDynamicTemplates(Map.of("location", "location"))
        );
        requests.add(
            new IndexRequest("test").id("3")
                .source("address.location", "41.12,-71.34")
                .setDynamicTemplates(Map.of("address.location", "location"))
        );
        requests.add(
            new IndexRequest("test").id("4")
                .source("location", new double[] { -71.34, 41.12 })
                .setDynamicTemplates(Map.of("location", "location"))
        );
        requests.add(new IndexRequest("test").id("5").source("array_of_numbers", new double[] { -71.34, 41.12 }));

        Randomness.shuffle(requests);
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        requests.forEach(bulkRequest::add);
        final BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());

        assertSearchHits(
            prepareSearch("test").setQuery(
                new GeoBoundingBoxQueryBuilder("location").setCorners(new GeoPoint(42, -72), new GeoPoint(40, -74))
            ),
            "1",
            "2",
            "4"
        );
        assertSearchHits(
            prepareSearch("test").setQuery(
                new GeoBoundingBoxQueryBuilder("address.location").setCorners(new GeoPoint(42, -72), new GeoPoint(40, -74))
            ),
            "3"
        );
    }

    public void testBulkRequestWithNotFoundDynamicTemplate() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test"));
        final XContentBuilder mappings = XContentFactory.jsonBuilder();
        mappings.startObject();
        {
            mappings.startArray("dynamic_templates");
            {
                if (randomBoolean()) {
                    mappings.startObject();
                    mappings.startObject("location");
                    {
                        if (randomBoolean()) {
                            mappings.field("match", "location");
                        }
                        if (randomBoolean()) {
                            mappings.field("match_mapping_type", "string");
                        }
                        mappings.startObject("mapping");
                        {
                            mappings.field("type", "geo_point");
                        }
                        mappings.endObject();
                    }
                    mappings.endObject();
                    mappings.endObject();
                }
            }
            mappings.endArray();
        }
        mappings.endObject();

        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulkRequest.add(
            new IndexRequest("test").id("1")
                .source(XContentFactory.jsonBuilder().startObject().field("my_location", "41.12,-71.34").endObject())
                .setDynamicTemplates(Map.of("my_location", "foo_bar")),
            new IndexRequest("test").id("2")
                .source(XContentFactory.jsonBuilder().startObject().field("address.location", "41.12,-71.34").endObject())
                .setDynamicTemplates(Map.of("address.location", "bar_foo"))
        );
        final BulkResponse bulkItemResponses = client().bulk(bulkRequest).actionGet();
        assertTrue(bulkItemResponses.hasFailures());
        assertThat(bulkItemResponses.getItems()[0].getFailure().getCause(), instanceOf(DocumentParsingException.class));
        assertThat(
            bulkItemResponses.getItems()[0].getFailureMessage(),
            containsString("Can't find dynamic template for dynamic template name [foo_bar] of field [my_location]")
        );
        assertThat(bulkItemResponses.getItems()[1].getFailure().getCause(), instanceOf(DocumentParsingException.class));
        assertThat(
            bulkItemResponses.getItems()[1].getFailureMessage(),
            containsString("[1:21] Can't find dynamic template for dynamic template name [bar_foo] of field [address.location]")
        );
    }

    public void testDynamicRuntimeNoConflicts() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("""
            {"_doc":{"dynamic":"runtime"}}""").get());

        List<IndexRequest> docs = new ArrayList<>();
        // the root is mapped dynamic:runtime hence there are no type conflicts
        docs.add(new IndexRequest("test").source("one.two.three", new int[] { 1, 2, 3 }));
        docs.add(new IndexRequest("test").source("one.two", 3.5));
        docs.add(new IndexRequest("test").source("one", "one"));
        docs.add(new IndexRequest("test").source("""
            {"one":{"two": { "three": "three"}}}""", XContentType.JSON));
        Collections.shuffle(docs, random());
        BulkRequest bulkRequest = new BulkRequest();
        for (IndexRequest doc : docs) {
            bulkRequest.add(doc);
        }
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        BulkResponse bulkItemResponses = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkItemResponses.buildFailureMessage(), bulkItemResponses.hasFailures());

        assertHitCount(prepareSearch("test").setQuery(new MatchQueryBuilder("one", "one")), 1);
        assertHitCount(prepareSearch("test").setQuery(new MatchQueryBuilder("one.two", 3.5)), 1);
        assertHitCount(prepareSearch("test").setQuery(new MatchQueryBuilder("one.two.three", "1")), 1);
    }

    public void testDynamicRuntimeObjectFields() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("""
            {
              "_doc": {
                "properties": {
                  "obj": {
                    "properties": {
                      "runtime": {
                        "type": "object",
                        "dynamic": "runtime"
                      }
                    }
                  }
                }
              }
            }""").get());

        List<IndexRequest> docs = new ArrayList<>();
        docs.add(new IndexRequest("test").source("obj.one", 1));
        docs.add(new IndexRequest("test").source("anything", "anything"));
        // obj.runtime is mapped dynamic:runtime hence there are no type conflicts
        docs.add(new IndexRequest("test").source("obj.runtime.one.two", "test"));
        docs.add(new IndexRequest("test").source("obj.runtime.one", "one"));
        docs.add(new IndexRequest("test").source("""
            {"obj":{"runtime":{"one":{"two": 1}}}}""", XContentType.JSON));
        Collections.shuffle(docs, random());
        BulkRequest bulkRequest = new BulkRequest();
        for (IndexRequest doc : docs) {
            bulkRequest.add(doc);
        }
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        BulkResponse bulkItemResponses = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkItemResponses.buildFailureMessage(), bulkItemResponses.hasFailures());

        assertHitCount(prepareSearch("test").setQuery(new MatchQueryBuilder("obj.one", 1)), 1);
        assertHitCount(prepareSearch("test").setQuery(new MatchQueryBuilder("anything", "anything")), 1);
        assertHitCount(prepareSearch("test").setQuery(new MatchQueryBuilder("obj.runtime.one", "one")), 1);
        assertHitCount(prepareSearch("test").setQuery(new MatchQueryBuilder("obj.runtime.one.two", "1")), 1);

        Exception exception = expectThrows(DocumentParsingException.class, prepareIndex("test").setSource("obj.runtime", "value"));
        assertThat(
            exception.getMessage(),
            containsString("object mapping for [obj.runtime] tried to parse field [runtime] as object, but found a concrete value")
        );

        assertAcked(indicesAdmin().preparePutMapping("test").setSource("""
            {
              "_doc": {
                "properties": {
                  "obj": {
                    "properties": {
                      "runtime": {
                        "properties": {
                          "dynamic": {
                            "type": "object",
                            "dynamic": true
                          }
                        }
                      }
                    }
                  }
                }
              }
            }""", XContentType.JSON));

        // the parent object has been mapped dynamic:true, hence the field gets indexed
        // we use a fixed doc id here to make sure this document and the one we sent later with a conflicting type
        // target the same shard where we are sure the mapping update has been applied
        assertEquals(
            RestStatus.CREATED,
            prepareIndex("test").setSource("obj.runtime.dynamic.number", 1)
                .setId("id")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get()
                .status()
        );

        assertHitCount(prepareSearch("test").setQuery(new MatchQueryBuilder("obj.runtime.dynamic.number", 1)), 1);

        // a doc with the same field but a different type causes a conflict
        Exception e = expectThrows(
            DocumentParsingException.class,
            prepareIndex("test").setId("id").setSource("obj.runtime.dynamic.number", "string")
        );
        assertThat(
            e.getMessage(),
            containsString(
                "failed to parse field [obj.runtime.dynamic.number] of type [long] in document with id 'id'. "
                    + "Preview of field's value: 'string'"
            )
        );
    }

    public void testSubobjectsFalseAtRoot() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("""
            {
              "_doc": {
                "subobjects" : false,
                "properties": {
                  "host.name": {
                    "type": "keyword"
                  }
                }
              }
            }""").get());

        IndexRequest request = new IndexRequest("test").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source("host.name", "localhost", "host.id", 111, "time", 100, "time.max", 1000);
        DocWriteResponse indexResponse = client().index(request).actionGet();
        assertEquals(RestStatus.CREATED, indexResponse.status());

        assertBusy(() -> {
            Map<String, Object> mappings = indicesAdmin().prepareGetMappings("test").get().mappings().get("test").sourceAsMap();
            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            assertEquals(4, properties.size());
            assertNotNull(properties.get("host.name"));
            assertNotNull(properties.get("host.id"));
            assertNotNull(properties.get("time"));
            assertNotNull(properties.get("time.max"));
        });

    }

    @SuppressWarnings("unchecked")
    public void testSubobjectsFalse() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("""
            {
              "_doc": {
                "properties": {
                  "foo.metrics" : {
                    "subobjects" : false,
                    "properties" : {
                      "host.name": {
                        "type": "keyword"
                      }
                    }
                  }
                }
              }
            }""").get());

        IndexRequest request = new IndexRequest("test").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(
                "foo.metrics.host.name",
                "localhost",
                "foo.metrics.host.id",
                111,
                "foo.metrics.time",
                100,
                "foo.metrics.time.max",
                1000
            );
        DocWriteResponse indexResponse = client().index(request).actionGet();
        assertEquals(RestStatus.CREATED, indexResponse.status());

        assertBusy(() -> {
            Map<String, Object> mappings = indicesAdmin().prepareGetMappings("test").get().mappings().get("test").sourceAsMap();
            Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
            Map<String, Object> foo = (Map<String, Object>) properties.get("foo");
            properties = (Map<String, Object>) foo.get("properties");
            Map<String, Object> metrics = (Map<String, Object>) properties.get("metrics");
            properties = (Map<String, Object>) metrics.get("properties");
            assertEquals(4, properties.size());
            assertNotNull(properties.get("host.name"));
            assertNotNull(properties.get("host.id"));
            assertNotNull(properties.get("time"));
            assertNotNull(properties.get("time.max"));
        });
    }

    public void testKnnSubObject() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("""
            {
              "properties": {
                "obj": {
                  "type": "object",
                  "dynamic": "true"
                },
                "mapped_obj": {
                  "type": "object",
                  "dynamic": "true",
                  "properties": {
                    "vector": {
                      "type": "dense_vector"
                    }
                  }
                }
              }
            }""").get());

        client().index(new IndexRequest("test").source("mapped_obj.vector", Randomness.get().doubles(3, 0.0, 5.0).toArray())).get();

        client().index(
            new IndexRequest("test").source("obj.vector", Randomness.get().doubles(MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING, 0.0, 5.0).toArray())
        ).get();

    }
}
