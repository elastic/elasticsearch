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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.indices.InvalidTypeNameException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

    public void testTypeValidation() {
        InvalidTypeNameException e = expectThrows(InvalidTypeNameException.class, () -> MapperService.validateTypeName("_type"));
        assertEquals("mapping type name [_type] can't start with '_' unless it is called [_doc]", e.getMessage());

        e = expectThrows(InvalidTypeNameException.class, () -> MapperService.validateTypeName("_document"));
        assertEquals("mapping type name [_document] can't start with '_' unless it is called [_doc]", e.getMessage());

        MapperService.validateTypeName("_doc"); // no exception
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
        assertNull(indexService.mapperService().documentMapper(MapperService.DEFAULT_MAPPING));
    }

    public void testTotalFieldsExceedsLimit() throws Throwable {
        Function<String, String> mapping = type -> {
            try {
                return Strings.toString(XContentFactory.jsonBuilder().startObject().startObject(type).startObject("properties")
                    .startObject("field1").field("type", "keyword")
                    .endObject().endObject().endObject().endObject());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        createIndex("test1").mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_UPDATE);
        //set total number of fields to 1 to trigger an exception
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            createIndex("test2", Settings.builder().put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1).build())
                .mapperService().merge("type", new CompressedXContent(mapping.apply("type")), MergeReason.MAPPING_UPDATE);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Limit of total fields [1] in index [test2] has been exceeded"));
    }

    public void testMappingDepthExceedsLimit() throws Throwable {
        CompressedXContent simpleMapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("field")
                        .field("type", "text")
                    .endObject()
                .endObject().endObject()));
        IndexService indexService1 = createIndex("test1", Settings.builder().put(MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getKey(), 1).build());
        // no exception
        indexService1.mapperService().merge("type", simpleMapping, MergeReason.MAPPING_UPDATE);

        CompressedXContent objectMapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("object1")
                        .field("type", "object")
                    .endObject()
                .endObject().endObject()));

        IndexService indexService2 = createIndex("test2");
        // no exception
        indexService2.mapperService().merge("type", objectMapping, MergeReason.MAPPING_UPDATE);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> indexService1.mapperService().merge("type", objectMapping, MergeReason.MAPPING_UPDATE));
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
            () -> mapperService.merge(mappings, MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), startsWith("Failed to parse mapping [" + MapperService.DEFAULT_MAPPING + "]: "));

        mappings.clear();
        mappings.put("type1", MapperService.parseMapping(xContentRegistry(), "{}"));

        e = expectThrows( MapperParsingException.class,
            () -> mapperService.merge(mappings, MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), startsWith("Failed to parse mapping [type1]: "));
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
        CompressedXContent nestedFieldMapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
            .startObject("nested_field")
            .field("type", "nested")
            .endObject()
            .endObject().endObject()));
        invalidNestedException = expectThrows(IllegalArgumentException.class,
            () -> indexService.mapperService().merge("t", nestedFieldMapping,
                MergeReason.MAPPING_UPDATE));
        assertThat(invalidNestedException.getMessage(),
            containsString("cannot have nested fields when index sort is activated"));
    }

     public void testFieldAliasWithMismatchedNestedScope() throws Throwable {
        IndexService indexService = createIndex("test");
        MapperService mapperService = indexService.mapperService();

        CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("nested")
                        .field("type", "nested")
                        .startObject("properties")
                            .startObject("field")
                                .field("type", "text")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()));

        mapperService.merge("type", mapping, MergeReason.MAPPING_UPDATE);

        CompressedXContent mappingUpdate = new CompressedXContent(BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("alias")
                        .field("type", "alias")
                        .field("path", "nested.field")
                    .endObject()
                .endObject()
            .endObject()));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> mapperService.merge("type", mappingUpdate, MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), containsString("Invalid [path] value [nested.field] for field alias [alias]"));
    }

    public void testTotalFieldsLimitWithFieldAlias() throws Throwable {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
                .startObject("alias")
                    .field("type", "alias")
                    .field("path", "field")
                .endObject()
                .startObject("field")
                    .field("type", "text")
                .endObject()
            .endObject()
        .endObject().endObject());

        DocumentMapper documentMapper = createIndex("test1").mapperService()
            .merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        // Set the total fields limit to the number of non-alias fields, to verify that adding
        // a field alias pushes the mapping over the limit.
        int numFields = documentMapper.mapping().metadataMappers.length + 2;
        int numNonAliasFields = numFields - 1;

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            Settings settings = Settings.builder()
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), numNonAliasFields)
                .build();
            createIndex("test2", settings).mapperService()
                .merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        });
        assertEquals("Limit of total fields [" + numNonAliasFields + "] in index [test2] has been exceeded", e.getMessage());
    }

    public void testForbidMultipleTypes() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type2").endObject().endObject());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService.merge("type2", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), Matchers.startsWith("Rejecting mapping update to [test] as the final mapping would have more than 1 type: "));
    }

    /**
     * This test checks that the multi-type validation is done before we do any other kind of validation on the mapping that's added,
     * see https://github.com/elastic/elasticsearch/issues/29313
     */
    public void testForbidMultipleTypesWithConflictingMappings() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field1").field("type", "integer_range").endObject().endObject().endObject().endObject());
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type2")
            .startObject("properties").startObject("field1").field("type", "integer").endObject().endObject().endObject().endObject());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> mapperService.merge("type2", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), Matchers.startsWith("Rejecting mapping update to [test] as the final mapping would have more than 1 type: "));
    }

    public void testDefaultMappingIsRejectedOn7() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_default_").endObject().endObject());
        MapperService mapperService = createIndex("test").mapperService();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService.merge("_default_", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE));
        assertEquals("The [default] mapping cannot be updated on index [test]: defaults mappings are not useful anymore now that indices " +
                "can have at most one type.", e.getMessage());
    }

}
