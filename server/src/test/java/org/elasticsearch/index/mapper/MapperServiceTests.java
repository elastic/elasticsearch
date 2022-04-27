/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MapperServiceTests extends MapperServiceTestCase {

    public void testPreflightUpdateDoesNotChangeMapping() throws Throwable {
        final MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(mapperService, MergeReason.MAPPING_UPDATE_PREFLIGHT, mapping(b -> createMappingSpecifyingNumberOfFields(b, 1)));
        assertThat("field was not created by preflight check", mapperService.fieldType("field0"), nullValue());
        merge(mapperService, MergeReason.MAPPING_UPDATE, mapping(b -> createMappingSpecifyingNumberOfFields(b, 1)));
        assertThat("field was not created by mapping update", mapperService.fieldType("field0"), notNullValue());
    }

    public void testMappingLookup() throws IOException {
        MapperService service = createMapperService(mapping(b -> {}));
        MappingLookup oldLookup = service.mappingLookup();
        assertThat(oldLookup.fieldTypesLookup().get("cat"), nullValue());

        merge(service, mapping(b -> b.startObject("cat").field("type", "keyword").endObject()));
        MappingLookup newLookup = service.mappingLookup();
        assertThat(newLookup.fieldTypesLookup().get("cat"), not(nullValue()));
        assertThat(oldLookup.fieldTypesLookup().get("cat"), nullValue());
    }

    /**
     * Test that we can have at least the number of fields in new mappings that are defined by "index.mapping.total_fields.limit".
     * Any additional field should trigger an IllegalArgumentException.
     */
    public void testTotalFieldsLimit() throws Throwable {
        int totalFieldsLimit = randomIntBetween(1, 10);
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), totalFieldsLimit)
            .build();
        MapperService mapperService = createMapperService(
            settings,
            mapping(b -> createMappingSpecifyingNumberOfFields(b, totalFieldsLimit))
        );

        // adding one more field should trigger exception
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, mapping(b -> b.startObject("newfield").field("type", "long").endObject()))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("Limit of total fields [" + totalFieldsLimit + "] has been exceeded"));
    }

    private void createMappingSpecifyingNumberOfFields(XContentBuilder b, int numberOfFields) throws IOException {
        for (int i = 0; i < numberOfFields; i++) {
            b.startObject("field" + i);
            b.field("type", randomFrom("long", "integer", "date", "keyword", "text"));
            b.endObject();
        }
    }

    public void testMappingDepthExceedsLimit() throws Throwable {

        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getKey(), 1).build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping((b -> {
            b.startObject("object1");
            b.field("type", "object");
            b.endObject();
        }))));
        assertThat(e.getMessage(), containsString("Limit of mapping depth [1] has been exceeded"));
    }

    public void testPartitionedConstraints() throws IOException {
        // partitioned index must have routing
        Settings settings = Settings.builder().put("index.number_of_shards", 4).put("index.routing_partition_size", 2).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(settings, mapping(b -> {})));
        assertThat(e.getMessage(), containsString("must have routing"));

        // valid partitioned index
        createMapperService(settings, topMapping(b -> b.startObject("_routing").field("required", true).endObject()));
    }

    public void testIndexSortWithNestedFields() throws IOException {
        Settings settings = Settings.builder().put("index.sort.field", "foo").build();
        IllegalArgumentException invalidNestedException = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(settings, mapping(b -> {
                b.startObject("nested_field").field("type", "nested").endObject();
                b.startObject("foo").field("type", "keyword").endObject();
            }))
        );

        assertThat(invalidNestedException.getMessage(), containsString("cannot have nested fields when index sort is activated"));

        MapperService mapperService = createMapperService(
            settings,
            mapping(b -> b.startObject("foo").field("type", "keyword").endObject())
        );
        invalidNestedException = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("nested_field");
            b.field("type", "nested");
            b.endObject();
        })));
        assertThat(invalidNestedException.getMessage(), containsString("cannot have nested fields when index sort is activated"));
    }

    public void testFieldAliasWithMismatchedNestedScope() throws Throwable {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("nested");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("field").field("type", "text").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("alias");
            {
                b.field("type", "alias");
                b.field("path", "nested.field");
            }
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Invalid [path] value [nested.field] for field alias [alias]"));
    }

    public void testTotalFieldsLimitWithFieldAlias() throws Throwable {

        int numberOfFieldsIncludingAlias = 2;

        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), numberOfFieldsIncludingAlias)
            .build();
        createMapperService(settings, mapping(b -> {
            b.startObject("alias").field("type", "alias").field("path", "field").endObject();
            b.startObject("field").field("type", "text").endObject();
        }));

        // Set the total fields limit to the number of non-alias fields, to verify that adding
        // a field alias pushes the mapping over the limit.
        int numberOfNonAliasFields = 1;
        Settings errorSettings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), numberOfNonAliasFields)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createMapperService(errorSettings, mapping(b -> {
            b.startObject("alias").field("type", "alias").field("path", "field").endObject();
            b.startObject("field").field("type", "text").endObject();
        })));
        assertEquals("Limit of total fields [" + numberOfNonAliasFields + "] has been exceeded", e.getMessage());
    }

    public void testFieldNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(25, 30);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createMapperService(settings, fieldMapping(b -> b.field("type", "text")));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, mapping(b -> b.startObject(testString).field("type", "text").endObject()))
        );

        assertEquals("Field name [" + testString + "] is longer than the limit of [" + maxFieldNameLength + "] characters", e.getMessage());
    }

    public void testObjectNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(25, 30);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, mapping(b -> b.startObject(testString).field("type", "object").endObject()))
        );

        assertEquals("Field name [" + testString + "] is longer than the limit of [" + maxFieldNameLength + "] characters", e.getMessage());
    }

    public void testAliasFieldNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(25, 30);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject(testString).field("type", "alias").field("path", "field").endObject();
            b.startObject("field").field("type", "text").endObject();
        })));

        assertEquals("Field name [" + testString + "] is longer than the limit of [" + maxFieldNameLength + "] characters", e.getMessage());
    }

    public void testMappingRecoverySkipFieldNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(25, 30);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));

        CompressedXContent mapping = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject(testString)
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        DocumentMapper documentMapper = mapperService.merge("_doc", mapping, MergeReason.MAPPING_RECOVERY);

        assertEquals(testString, documentMapper.mappers().getMapper(testString).simpleName());
    }

    public void testIsMetadataField() throws IOException {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();

        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        assertFalse(mapperService.isMetadataField(randomAlphaOfLengthBetween(10, 15)));

        for (String builtIn : IndicesModule.getBuiltInMetadataFields()) {
            if (NestedPathFieldMapper.NAME.equals(builtIn) && version.before(Version.V_8_0_0)) {
                continue;   // Nested field does not exist in the 7x line
            }
            assertTrue("Expected " + builtIn + " to be a metadata field for version " + version, mapperService.isMetadataField(builtIn));
        }
    }

    public void testMappingUpdateChecks() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "text")));

        {
            IndexMetadata.Builder builder = new IndexMetadata.Builder("test");
            Settings settings = Settings.builder()
                .put("index.number_of_replicas", 0)
                .put("index.number_of_shards", 1)
                .put("index.version.created", Version.CURRENT)
                .build();
            builder.settings(settings);

            // Text fields are not stored by default, so an incoming update that is identical but
            // just has `stored:false` should not require an update
            builder.putMapping("""
                {"properties":{"field":{"type":"text","store":"false"}}}""");
            assertTrue(mapperService.assertNoUpdateRequired(builder.build()));
        }

        {
            IndexMetadata.Builder builder = new IndexMetadata.Builder("test");
            Settings settings = Settings.builder()
                .put("index.number_of_replicas", 0)
                .put("index.number_of_shards", 1)
                .put("index.version.created", Version.CURRENT)
                .build();
            builder.settings(settings);

            // However, an update that really does need a rebuild will throw an exception
            builder.putMapping("""
                {"properties":{"field":{"type":"text","store":"true"}}}""");
            Exception e = expectThrows(IllegalStateException.class, () -> mapperService.assertNoUpdateRequired(builder.build()));

            assertThat(e.getMessage(), containsString("expected current mapping ["));
            assertThat(e.getMessage(), containsString("to be the same as new mapping"));
        }
    }

    public void testEagerGlobalOrdinals() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("eager1").field("type", "keyword").field("eager_global_ordinals", true).endObject();
            b.startObject("lazy1").field("type", "keyword").field("eager_global_ordinals", false).endObject();
            b.startObject("eager2").field("type", "keyword").field("eager_global_ordinals", true).endObject();
            b.startObject("lazy2").field("type", "long").endObject();
        }));

        List<String> eagerFieldNames = StreamSupport.stream(mapperService.getEagerGlobalOrdinalsFields().spliterator(), false)
            .map(MappedFieldType::name)
            .toList();
        assertThat(eagerFieldNames, containsInAnyOrder("eager1", "eager2"));
    }

    public void testMultiFieldChecks() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : {
              "properties" : {
                 "field1" : {
                   "type" : "keyword",
                   "fields" : {
                     "subfield1" : {
                       "type" : "long"
                     },
                     "subfield2" : {
                       "type" : "text"
                     }
                   }
                 },
                 "object.field2" : { "type" : "keyword" }
              },
              "runtime" : {
                  "object.subfield1" : { "type" : "keyword" },
                  "field1.subfield2" : { "type" : "keyword" }
              }
            } }
            """);

        assertFalse(mapperService.isMultiField("non_existent_field"));
        assertFalse(mapperService.isMultiField("field1"));
        assertTrue(mapperService.isMultiField("field1.subfield1"));
        // not a multifield, because it's shadowed by a runtime field
        assertFalse(mapperService.isMultiField("field1.subfield2"));
        assertFalse(mapperService.isMultiField("object.field2"));
        assertFalse(mapperService.isMultiField("object.subfield1"));
    }

}
