/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class MapperServiceTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testTypeNameStartsWithIllegalDot() {
        String index = "test-index";
        String type = ".test-type";
        String field = "field";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            client().admin().indices().prepareCreate(index)
                    .addMapping(type, field, "type=text")
                    .execute().actionGet();
        });
        assertTrue(e.getMessage(), e.getMessage().contains("mapping type name [.test-type] must not start with a '.'"));
    }

    public void testTypeNameTooLong() {
        String index = "text-index";
        String field = "field";
        String type = new String(new char[256]).replace("\0", "a");

        MapperException e = expectThrows(MapperException.class, () -> {
            client().admin().indices().prepareCreate(index)
                    .addMapping(type, field, "type=text")
                    .execute().actionGet();
        });
        assertTrue(e.getMessage(), e.getMessage().contains("mapping type name [" + type + "] is too long; limit is length 255 but was [256]"));
    }

    public void testTypes() throws Exception {
        IndexService indexService1 = createIndex("index1", Settings.builder().put("index.version.created", Version.V_5_6_0) // multi types
            .build());
        MapperService mapperService = indexService1.mapperService();
        assertEquals(Collections.emptySet(), mapperService.types());

        mapperService.merge("type1", new CompressedXContent("{\"type1\":{}}"), MapperService.MergeReason.MAPPING_UPDATE, false);
        assertNull(mapperService.documentMapper(MapperService.DEFAULT_MAPPING));
        assertEquals(Collections.singleton("type1"), mapperService.types());

        mapperService.merge(MapperService.DEFAULT_MAPPING, new CompressedXContent("{\"_default_\":{}}"), MapperService.MergeReason.MAPPING_UPDATE, false);
        assertNotNull(mapperService.documentMapper(MapperService.DEFAULT_MAPPING));
        assertEquals(Collections.singleton("type1"), mapperService.types());

        mapperService.merge("type2", new CompressedXContent("{\"type2\":{}}"), MapperService.MergeReason.MAPPING_UPDATE, false);
        assertNotNull(mapperService.documentMapper(MapperService.DEFAULT_MAPPING));
        assertEquals(new HashSet<>(Arrays.asList("type1", "type2")), mapperService.types());
    }

    public void testIndexIntoDefaultMapping() throws Throwable {
        // 1. test implicit index creation
        ExecutionException e = expectThrows(ExecutionException.class, () -> {
            client().prepareIndex("index1", MapperService.DEFAULT_MAPPING, "1").setSource("{}", XContentType.JSON).execute().get();
        });
        Throwable throwable = ExceptionsHelper.unwrapCause(e.getCause());
        if (throwable instanceof IllegalArgumentException) {
            assertEquals("It is forbidden to index into the default mapping [_default_]", throwable.getMessage());
        } else {
            throw e;
        }

        // 2. already existing index
        IndexService indexService = createIndex("index2");
        e = expectThrows(ExecutionException.class, () -> {
            client().prepareIndex("index1", MapperService.DEFAULT_MAPPING, "2").setSource().execute().get();
        });
        throwable = ExceptionsHelper.unwrapCause(e.getCause());
        if (throwable instanceof IllegalArgumentException) {
            assertEquals("It is forbidden to index into the default mapping [_default_]", throwable.getMessage());
        } else {
            throw e;
        }
        assertFalse(indexService.mapperService().hasMapping(MapperService.DEFAULT_MAPPING));
    }

    public void testTotalFieldsExceedsLimit() throws Throwable {
        Function<String, String> mapping = type -> {
            try {
                return XContentFactory.jsonBuilder().startObject().startObject(type).startObject("properties")
                    .startObject("field1").field("type", "keyword")
                    .endObject().endObject().endObject().endObject().string();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        createIndex("test1").mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_UPDATE, false);
        //set total number of fields to 1 to trigger an exception
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            createIndex("test2", Settings.builder().put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1).build())
                .mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_UPDATE, false);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Limit of total fields [1] in index [test2] has been exceeded"));
    }

    public void testMappingDepthExceedsLimit() throws Throwable {
        CompressedXContent simpleMapping = new CompressedXContent(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("field")
                        .field("type", "text")
                    .endObject()
                .endObject().endObject().bytes());
        IndexService indexService1 = createIndex("test1", Settings.builder().put(MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getKey(), 1).build());
        // no exception
        indexService1.mapperService().merge("type", simpleMapping, MergeReason.MAPPING_UPDATE, false);

        CompressedXContent objectMapping = new CompressedXContent(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("object1")
                        .field("type", "object")
                    .endObject()
                .endObject().endObject().bytes());

        IndexService indexService2 = createIndex("test2");
        // no exception
        indexService2.mapperService().merge("type", objectMapping, MergeReason.MAPPING_UPDATE, false);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> indexService1.mapperService().merge("type2", objectMapping, MergeReason.MAPPING_UPDATE, false));
        assertThat(e.getMessage(), containsString("Limit of mapping depth [1] in index [test1] has been exceeded"));
    }

    public void testUnmappedFieldType() {
        MapperService mapperService = createIndex("index").mapperService();
        assertThat(mapperService.unmappedFieldType("keyword"), instanceOf(KeywordFieldType.class));
        assertThat(mapperService.unmappedFieldType("long"), instanceOf(NumberFieldType.class));
        // back compat
        assertThat(mapperService.unmappedFieldType("string"), instanceOf(KeywordFieldType.class));
        assertWarnings("[unmapped_type:string] should be replaced with [unmapped_type:keyword]");
    }

    public void testMergeWithMap() throws Throwable {
        IndexService indexService1 = createIndex("index1");
        MapperService mapperService = indexService1.mapperService();
        Map<String, Map<String, Object>> mappings = new HashMap<>();

        mappings.put(MapperService.DEFAULT_MAPPING, MapperService.parseMapping(xContentRegistry(), "{}"));
        MapperException e = expectThrows(MapperParsingException.class,
            () -> mapperService.merge(mappings, MergeReason.MAPPING_UPDATE, false));
        assertThat(e.getMessage(), startsWith("Failed to parse mapping [" + MapperService.DEFAULT_MAPPING + "]: "));

        mappings.clear();
        mappings.put("type1", MapperService.parseMapping(xContentRegistry(), "{}"));

        e = expectThrows( MapperParsingException.class,
            () -> mapperService.merge(mappings, MergeReason.MAPPING_UPDATE, false));
        assertThat(e.getMessage(), startsWith("Failed to parse mapping [type1]: "));
    }

    public void testMergeParentTypesSame() {
        // Verifies that a merge (absent a DocumentMapper change)
        // doesn't change the parentTypes reference.
        // The collection was being rewrapped with each merge
        // in v5.2 resulting in eventual StackOverflowErrors.
        // https://github.com/elastic/elasticsearch/issues/23604

        IndexService indexService1 = createIndex("index1");
        MapperService mapperService = indexService1.mapperService();
        Set<String> parentTypes = mapperService.getParentTypes();

        Map<String, Map<String, Object>> mappings = new HashMap<>();
        mapperService.merge(mappings, MergeReason.MAPPING_UPDATE, false);
        assertSame(parentTypes, mapperService.getParentTypes());
    }

    public void testOtherDocumentMappersOnlyUpdatedWhenChangingFieldType() throws IOException {
        IndexService indexService = createIndex("test",
            Settings.builder().put("index.version.created", Version.V_5_6_0).build()); // multiple types

        CompressedXContent simpleMapping = new CompressedXContent(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .endObject()
            .endObject().endObject().bytes());

        indexService.mapperService().merge("type1", simpleMapping, MergeReason.MAPPING_UPDATE, true);
        DocumentMapper documentMapper = indexService.mapperService().documentMapper("type1");

        indexService.mapperService().merge("type2", simpleMapping, MergeReason.MAPPING_UPDATE, true);
        assertSame(indexService.mapperService().documentMapper("type1"), documentMapper);

        CompressedXContent normsDisabledMapping = new CompressedXContent(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("norms", false)
            .endObject()
            .endObject().endObject().bytes());

        indexService.mapperService().merge("type3", normsDisabledMapping, MergeReason.MAPPING_UPDATE, true);
        assertNotSame(indexService.mapperService().documentMapper("type1"), documentMapper);
    }

    public void testAllEnabled() throws Exception {
        IndexService indexService = createIndex("test");
        assertFalse(indexService.mapperService().allEnabled());

        CompressedXContent enabledAll = new CompressedXContent(XContentFactory.jsonBuilder().startObject()
                .startObject("_all")
                    .field("enabled", true)
                .endObject().endObject().bytes());

        Exception e = expectThrows(MapperParsingException.class,
                () -> indexService.mapperService().merge(MapperService.DEFAULT_MAPPING, enabledAll,
                        MergeReason.MAPPING_UPDATE, random().nextBoolean()));
        assertThat(e.getMessage(), containsString("[_all] is disabled in 6.0"));
        assertWarnings("[_all] is deprecated in 6.0+ and will be removed in 7.0. As a replacement, " +
                        "you can use [copy_to] on mapping fields to create your own catch all field.");
    }

     public void testPartitionedConstraints() {
        // partitioned index must have routing
         IllegalArgumentException noRoutingException = expectThrows(IllegalArgumentException.class, () -> {
            client().admin().indices().prepareCreate("test-index")
                    .addMapping("type", "{\"type\":{}}", XContentType.JSON)
                    .setSettings(Settings.builder()
                        .put("index.number_of_shards", 4)
                        .put("index.routing_partition_size", 2))
                    .execute().actionGet();
        });
        assertTrue(noRoutingException.getMessage(), noRoutingException.getMessage().contains("must have routing"));

        // partitioned index cannot have parent/child relationships
        IllegalArgumentException parentException = expectThrows(IllegalArgumentException.class, () -> {
            client().admin().indices().prepareCreate("test-index")
                    .addMapping("parent", "{\"parent\":{\"_routing\":{\"required\":true}}}", XContentType.JSON)
                    .addMapping("child", "{\"child\": {\"_routing\":{\"required\":true}, \"_parent\": {\"type\": \"parent\"}}}",
                        XContentType.JSON)
                    .setSettings(Settings.builder()
                        .put("index.number_of_shards", 4)
                        .put("index.routing_partition_size", 2))
                    .execute().actionGet();
        });
        assertTrue(parentException.getMessage(), parentException.getMessage().contains("cannot have a _parent field"));

        // valid partitioned index
        assertTrue(client().admin().indices().prepareCreate("test-index")
            .addMapping("type", "{\"type\":{\"_routing\":{\"required\":true}}}", XContentType.JSON)
            .setSettings(Settings.builder()
                .put("index.number_of_shards", 4)
                .put("index.routing_partition_size", 2))
            .execute().actionGet().isAcknowledged());
    }

    public void testIndexSortWithNestedFields() throws IOException {
        Settings settings = Settings.builder()
            .put("index.sort.field", "foo")
            .build();
        IllegalArgumentException invalidNestedException = expectThrows(IllegalArgumentException.class,
           () -> createIndex("test", settings, "t", "nested_field", "type=nested", "foo", "type=keyword"));
        assertThat(invalidNestedException.getMessage(),
            containsString("cannot have nested fields when index sort is activated"));
        IndexService indexService =  createIndex("test", settings, "t", "foo", "type=keyword");
        CompressedXContent nestedFieldMapping = new CompressedXContent(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
            .startObject("nested_field")
            .field("type", "nested")
            .endObject()
            .endObject().endObject().bytes());
        invalidNestedException = expectThrows(IllegalArgumentException.class,
            () -> indexService.mapperService().merge("t", nestedFieldMapping,
                MergeReason.MAPPING_UPDATE, true));
        assertThat(invalidNestedException.getMessage(),
            containsString("cannot have nested fields when index sort is activated"));
    }

    public void testForbidMultipleTypes() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, randomBoolean());

        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type2").endObject().endObject().string();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService.merge("type2", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE, randomBoolean()));
        assertThat(e.getMessage(), Matchers.startsWith("Rejecting mapping update to [test] as the final mapping would have more than 1 type: "));
    }

    public void testDefaultMappingIsDeprecated() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("_default_").endObject().endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("_default_", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE, randomBoolean());
        assertWarnings("[_default_] mapping is deprecated since it is not useful anymore now that indexes " +
                "cannot have more than one type");
    }
}
