/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MapperServiceTests extends MapperServiceTestCase {

    public void testPreflightUpdateDoesNotChangeMapping() throws Throwable {
        final MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(mapperService, MergeReason.MAPPING_AUTO_UPDATE_PREFLIGHT, mapping(b -> createMappingSpecifyingNumberOfFields(b, 1)));
        assertThat("field was not created by preflight check", mapperService.fieldType("field0"), nullValue());
        merge(
            mapperService,
            randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.MAPPING_AUTO_UPDATE),
            mapping(b -> createMappingSpecifyingNumberOfFields(b, 1))
        );
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
            .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
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

        // adding one more runtime field should trigger exception
        e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, runtimeMapping(b -> b.startObject("newfield").field("type", "long").endObject()))
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
        IndexVersion oldVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.INDEX_SORTING_ON_NESTED);
        IllegalArgumentException invalidNestedException = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(oldVersion, settings(oldVersion).put("index.sort.field", "foo").build(), () -> true, mapping(b -> {
                b.startObject("nested_field").field("type", "nested").endObject();
                b.startObject("foo").field("type", "keyword").endObject();
            }))
        );

        Settings settings = settings(IndexVersions.INDEX_SORTING_ON_NESTED).put("index.sort.field", "foo").build();
        DocumentMapper mapper = createMapperService(settings, mapping(b -> {
            b.startObject("nested_field").field("type", "nested").endObject();
            b.startObject("foo").field("type", "keyword").endObject();
        })).documentMapper();

        List<LuceneDocument> docs = mapper.parse(source(b -> {
            b.field("name", "foo");
            b.startObject("nested_field").field("foo", "bar").endObject();
        })).docs();
        assertEquals(2, docs.size());
        assertEquals(docs.get(1), docs.get(0).getParent());

        MapperService mapperService = createMapperService(
            settings,
            mapping(b -> b.startObject("foo").field("type", "keyword").endObject())
        );
        merge(mapperService, mapping(b -> {
            b.startObject("nested_field");
            b.field("type", "nested");
            b.endObject();
        }));

        Settings settings2 = Settings.builder().put("index.sort.field", "foo.bar").build();
        invalidNestedException = expectThrows(IllegalArgumentException.class, () -> createMapperService(settings2, mapping(b -> {
            b.startObject("foo");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("bar").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertEquals("cannot apply index sort to field [foo.bar] under nested object [foo]", invalidNestedException.getMessage());
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
            .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
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
            .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
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

        assertEquals(testString, documentMapper.mappers().getMapper(testString).leafName());
    }

    public void testIsMetadataField() throws IOException {
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion(random());

        CheckedFunction<IndexMode, MapperService, IOException> initMapperService = (indexMode) -> {
            Settings.Builder settingsBuilder = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .put(IndexSettings.MODE.getKey(), indexMode);

            if (indexMode == IndexMode.TIME_SERIES) {
                settingsBuilder.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo");
            }

            return createMapperService(settingsBuilder.build(), mapping(b -> {}));
        };

        Consumer<MapperService> assertMapperService = (mapperService) -> {
            assertFalse(mapperService.isMetadataField(randomAlphaOfLengthBetween(10, 15)));

            for (String builtIn : IndicesModule.getBuiltInMetadataFields()) {
                if (NestedPathFieldMapper.NAME.equals(builtIn) && version.before(IndexVersions.V_8_0_0)) {
                    continue;  // Nested field does not exist in the 7x line
                }
                boolean isTimeSeriesField = builtIn.equals("_tsid") || builtIn.equals("_ts_routing_hash");
                boolean isTimeSeriesMode = mapperService.getIndexSettings().getMode().equals(IndexMode.TIME_SERIES);

                if (isTimeSeriesField && isTimeSeriesMode == false) {
                    assertFalse(
                        "Expected "
                            + builtIn
                            + " to not be a metadata field for version "
                            + version
                            + " and index mode "
                            + mapperService.getIndexSettings().getMode(),
                        mapperService.isMetadataField(builtIn)
                    );
                } else {
                    assertTrue(
                        "Expected "
                            + builtIn
                            + " to be a metadata field for version "
                            + version
                            + " and index mode "
                            + mapperService.getIndexSettings().getMode(),
                        mapperService.isMetadataField(builtIn)
                    );
                }
            }
        };

        for (IndexMode indexMode : IndexMode.values()) {
            MapperService mapperService = initMapperService.apply(indexMode);
            assertMapperService.accept(mapperService);
        }
    }

    public void testMappingUpdateChecks() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "text")));

        {
            IndexMetadata.Builder builder = new IndexMetadata.Builder("test");
            builder.settings(indexSettings(IndexVersion.current(), 1, 0));

            // Text fields are not stored by default, so an incoming update that is identical but
            // just has `stored:false` should not require an update
            builder.putMapping("""
                {"properties":{"field":{"type":"text","store":"false"}}}""");
            assertTrue(mapperService.assertNoUpdateRequired(builder.build()));
        }

        {
            IndexMetadata.Builder builder = new IndexMetadata.Builder("test");
            builder.settings(indexSettings(IndexVersion.current(), 1, 0));

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

    public void testMergeObjectSubfieldWhileParsing() throws IOException {
        /*
        If we are parsing mappings that hold the definition of the same field twice, the two are merged together. This can happen when
        mappings have the same field specified using the object notation as well as the dot notation, as well as when applying index
        templates, in which case the two definitions may come from separate index templates that end up in the same map (through
        XContentHelper#mergeDefaults, see MetadataCreateIndexService#parseV1Mappings).
        We had a bug (https://github.com/elastic/elasticsearch/issues/88573) triggered by this scenario that caused the merged leaf fields
        to get the wrong path (missing the first portion).
         */
        MapperService mapperService = createMapperService("""
            {
              "_doc": {
                "properties": {
                  "obj": {
                    "properties": {
                      "sub": {
                        "properties": {
                          "string": {
                            "type": "keyword"
                          }
                        }
                      }
                    }
                  },
                  "obj.sub.string" : {
                    "type" : "keyword"
                  }
                }
              }
            }
            """);

        assertNotNull(mapperService.mappingLookup().getMapper("obj.sub.string"));
        MappedFieldType fieldType = mapperService.mappingLookup().getFieldType("obj.sub.string");
        assertNotNull(fieldType);
        assertEquals("""
            {
              "_doc" : {
                "properties" : {
                  "obj" : {
                    "properties" : {
                      "sub" : {
                        "properties" : {
                          "string" : {
                            "type" : "keyword"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }""", Strings.toString(mapperService.documentMapper().mapping(), true, true));

        // check that with the resulting mappings a new document has the previously merged field indexed properly
        ParsedDocument parsedDocument = mapperService.documentMapper().parse(source("""
            {
              "obj.sub.string" : "value"
            }"""));

        assertNull(parsedDocument.dynamicMappingsUpdate());
        List<IndexableField> fields = parsedDocument.rootDoc().getFields("obj.sub.string");
        assertEquals(1, fields.size());
    }

    public void testBulkMerge() throws IOException {
        final MapperService mapperService = createMapperService(mapping(b -> {}));
        CompressedXContent mapping1 = createTestMapping1();
        CompressedXContent mapping2 = createTestMapping2();
        mapperService.merge("_doc", mapping1, MergeReason.INDEX_TEMPLATE);
        DocumentMapper sequentiallyMergedMapper = mapperService.merge("_doc", mapping2, MergeReason.INDEX_TEMPLATE);
        DocumentMapper bulkMergedMapper = mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE);
        assertEquals(sequentiallyMergedMapper.mappingSource(), bulkMergedMapper.mappingSource());
    }

    public void testMergeSubobjectsFalseOrder() throws IOException {
        final MapperService mapperService = createMapperService(mapping(b -> {}));
        CompressedXContent mapping1 = createTestMapping1();
        CompressedXContent mapping2 = createTestMapping2();
        DocumentMapper subobjectsFirst = mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE);
        DocumentMapper subobjectsLast = mapperService.merge("_doc", List.of(mapping2, mapping1), MergeReason.INDEX_TEMPLATE);
        assertEquals(subobjectsFirst.mappingSource(), subobjectsLast.mappingSource());
    }

    private static CompressedXContent createTestMapping1() throws IOException {
        CompressedXContent mapping1;
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            mapping1 = new CompressedXContent(
                BytesReference.bytes(
                    xContentBuilder.startObject()
                        .startObject("_doc")
                        .field("subobjects", false)
                        .startObject("properties")
                        .startObject("parent")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
            );
        }
        return mapping1;
    }

    private static CompressedXContent createTestMapping2() throws IOException {
        CompressedXContent mapping2;
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            mapping2 = new CompressedXContent(
                BytesReference.bytes(
                    xContentBuilder.startObject()
                        .startObject("_doc")
                        .field("subobjects", false)
                        .startObject("properties")
                        .startObject("parent.subfield")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
            );
        }
        return mapping2;
    }

    public void testSubobjectsDisabledNotAtRoot() throws IOException {
        final MapperService mapperService = createMapperService(mapping(b -> {}));
        CompressedXContent mapping1;
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            mapping1 = new CompressedXContent(
                BytesReference.bytes(
                    xContentBuilder.startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject("parent")
                        .field("subobjects", false)
                        .field("type", "object")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
            );
        }
        CompressedXContent mapping2;
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            mapping2 = new CompressedXContent(
                BytesReference.bytes(
                    xContentBuilder.startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject("parent")
                        .startObject("properties")
                        .startObject("child.grandchild")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
            );
        }

        DocumentMapper subobjectsFirst = mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE);
        DocumentMapper subobjectsLast = mapperService.merge("_doc", List.of(mapping2, mapping1), MergeReason.INDEX_TEMPLATE);
        assertEquals(subobjectsFirst.mappingSource(), subobjectsLast.mappingSource());
    }

    public void testMergeMultipleRoots() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties" : {
                "field" : {
                  "subobjects" : false,
                  "type" : "object"
                }
              }
            }
            """);

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "_doc" : {
                "_meta" : {
                  "meta-field" : "some-info"
                },
                "properties" : {
                  "field" : {
                    "properties" : {
                      "subfield" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "_meta" : {
                  "meta-field" : "some-info"
                },
                "properties" : {
                  "field" : {
                    "subobjects" : false,
                    "properties" : {
                      "subfield" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testMergeMultipleRootsWithRootType() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties" : {
                "field" : {
                  "type" : "keyword"
                }
              }
            }
            """);

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "_doc" : {
                "_meta" : {
                  "meta-field" : "some-info"
                }
              },
              "properties" : {
                "field" : {
                  "subobjects" : false
                }
              }
            }""");

        final MapperService mapperService = createMapperService(mapping(b -> {}));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE)
        );
        assertThat(e.getMessage(), containsString("cannot merge a map with multiple roots, one of which is [_doc]"));
    }

    public void testMergeMultipleRootsWithoutRootType() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties" : {
                "field" : {
                  "type" : "keyword"
                }
              }
            }
            """);

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "_meta" : {
                "meta-field" : "some-info"
              }
            }""");

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "_meta" : {
                  "meta-field" : "some-info"
                },
                "properties" : {
                  "field" : {
                    "type" : "keyword"
                  }
                }
              }
            }""");
    }

    public void testValidMappingSubstitution() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "field": {
                  "type": "keyword",
                  "ignore_above": 1024
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "field": {
                  "type": "long",
                  "coerce": true
                }
              }
            }""");

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "properties" : {
                  "field" : {
                    "type" : "long",
                    "coerce" : true
                  }
                }
              }
            }""");
    }

    public void testValidMappingSubtreeSubstitution() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "field": {
                  "type": "object",
                                                                      "subobjects": false,
                  "properties": {
                    "subfield": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "field": {
                  "type": "long",
                  "coerce": true
                }
              }
            }""");

        final MapperService mapperService = createMapperService(mapping(b -> {}));
        mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE);

        assertEquals("""
            {
              "_doc" : {
                "properties" : {
                  "field" : {
                    "type" : "long",
                    "coerce" : true
                  }
                }
              }
            }""", Strings.toString(mapperService.documentMapper().mapping(), true, true));
    }

    public void testSameTypeMerge() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "field": {
                  "type": "keyword",
                  "ignore_above": 256,
                  "doc_values": false,
                  "fields": {
                    "text": {
                      "type": "text"
                    }
                  }
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "field": {
                  "type": "keyword",
                  "ignore_above": 1024,
                  "fields": {
                    "other_text": {
                      "type": "text"
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "properties" : {
                  "field" : {
                    "type" : "keyword",
                    "ignore_above" : 1024,
                    "fields" : {
                      "other_text" : {
                        "type" : "text"
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testObjectAndNestedTypeSubstitution() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties" : {
                "field": {
                  "type": "nested",
                  "include_in_parent": true,
                  "properties": {
                    "subfield1": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "field": {
                  "type": "object",
                  "properties": {
                    "subfield2": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""");

        final MapperService mapperService = createMapperService(mapping(b -> {}));
        mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE);

        assertEquals("""
            {
              "_doc" : {
                "properties" : {
                  "field" : {
                    "properties" : {
                      "subfield1" : {
                        "type" : "keyword"
                      },
                      "subfield2" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""", Strings.toString(mapperService.documentMapper().mapping(), true, true));
    }

    public void testNestedContradictingProperties() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "field": {
                  "type": "nested",
                  "include_in_parent": false,
                  "properties": {
                    "subfield1": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "field": {
                  "type": "nested",
                  "include_in_parent": true,
                  "properties": {
                    "subfield2": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "properties" : {
                  "field" : {
                    "type" : "nested",
                    "include_in_parent" : true,
                    "properties" : {
                      "subfield1" : {
                        "type" : "keyword"
                      },
                      "subfield2" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testImplicitObjectHierarchy() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "parent": {
                  "properties": {
                    "child.grandchild": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping1), """
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "properties" : {
                      "child" : {
                        "properties" : {
                          "grandchild" : {
                            "type" : "keyword"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testSubobjectsMerge() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "parent": {
                  "type": "object",
                  "subobjects": false
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "parent": {
                  "properties": {
                    "child.grandchild": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""");

        final MapperService mapperService = createMapperService(mapping(b -> {}));
        mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE);

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "subobjects" : false,
                    "properties" : {
                      "child.grandchild" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testContradictingSubobjects() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "parent": {
                  "type": "object",
                  "subobjects": false,
                  "properties": {
                    "child.grandchild": {
                      "type": "text"
                    }
                  }
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "parent": {
                  "type": "object",
                  "subobjects": true,
                  "properties": {
                    "child.grandchild": {
                      "type": "long"
                    }
                  }
                }
              }
            }""");

        MapperService mapperService = createMapperService(mapping(b -> {}));
        mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE);

        assertEquals("""
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "subobjects" : true,
                    "properties" : {
                      "child" : {
                        "properties" : {
                          "grandchild" : {
                            "type" : "long"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }""", Strings.toString(mapperService.documentMapper().mapping(), true, true));

        mapperService = createMapperService(mapping(b -> {}));
        mapperService.merge("_doc", List.of(mapping2, mapping1), MergeReason.INDEX_TEMPLATE);

        assertMergeEquals(List.of(mapping2, mapping1), """
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "subobjects" : false,
                    "properties" : {
                      "child.grandchild" : {
                        "type" : "text"
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testSubobjectsImplicitObjectsMerge() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "parent": {
                  "type": "object",
                  "subobjects": false
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "parent.child.grandchild": {
                  "type": "keyword"
                }
              }
            }""");

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "subobjects" : false,
                    "properties" : {
                      "child.grandchild" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testMultipleTypeMerges() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties" : {
                "parent": {
                  "type": "object",
                  "properties": {
                    "child": {
                      "type": "object",
                      "properties": {
                        "grandchild1": {
                          "type": "keyword"
                        },
                        "grandchild2": {
                          "type": "date"
                        }
                      }
                    }
                  }
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties" : {
                "parent": {
                  "type": "object",
                  "properties": {
                    "child": {
                      "type": "nested",
                      "properties": {
                        "grandchild1": {
                          "type": "text"
                        },
                        "grandchild3": {
                          "type": "text"
                        }
                      }
                    }
                  }
                }
              }
            }""");

        final MapperService mapperService = createMapperService(mapping(b -> {}));
        mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE);

        assertEquals("""
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "properties" : {
                      "child" : {
                        "type" : "nested",
                        "properties" : {
                          "grandchild1" : {
                            "type" : "text"
                          },
                          "grandchild2" : {
                            "type" : "date"
                          },
                          "grandchild3" : {
                            "type" : "text"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }""", Strings.toString(mapperService.documentMapper().mapping(), true, true));
    }

    public void testPropertiesFieldSingleChildMerge() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "properties": {
                  "type": "object",
                  "properties": {
                    "child": {
                      "type": "object",
                      "dynamic": true,
                      "properties": {
                        "grandchild": {
                          "type": "keyword"
                        }
                      }
                    }
                  }
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "properties": {
                  "properties": {
                    "child": {
                      "type": "long",
                      "coerce": true
                    }
                  }
                }
              }
            }""");

        MapperService mapperService = createMapperService(mapping(b -> {}));
        mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE);
        assertEquals("""
            {
              "_doc" : {
                "properties" : {
                  "properties" : {
                    "properties" : {
                      "child" : {
                        "type" : "long",
                        "coerce" : true
                      }
                    }
                  }
                }
              }
            }""", Strings.toString(mapperService.documentMapper().mapping(), true, true));

        Mapper propertiesMapper = mapperService.documentMapper().mapping().getRoot().getMapper("properties");
        assertThat(propertiesMapper, instanceOf(ObjectMapper.class));
        Mapper childMapper = ((ObjectMapper) propertiesMapper).getMapper("child");
        assertThat(childMapper, instanceOf(FieldMapper.class));
        assertEquals("long", childMapper.typeName());

        // Now checking the opposite merge
        mapperService = createMapperService(mapping(b -> {}));
        mapperService.merge("_doc", List.of(mapping2, mapping1), MergeReason.INDEX_TEMPLATE);
        assertEquals("""
            {
              "_doc" : {
                "properties" : {
                  "properties" : {
                    "properties" : {
                      "child" : {
                        "dynamic" : "true",
                        "properties" : {
                          "grandchild" : {
                            "type" : "keyword"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }""", Strings.toString(mapperService.documentMapper().mapping(), true, true));

        propertiesMapper = mapperService.documentMapper().mapping().getRoot().getMapper("properties");
        assertThat(propertiesMapper, instanceOf(ObjectMapper.class));
        childMapper = ((ObjectMapper) propertiesMapper).getMapper("child");
        assertThat(childMapper, instanceOf(ObjectMapper.class));
        Mapper grandchildMapper = ((ObjectMapper) childMapper).getMapper("grandchild");
        assertThat(grandchildMapper, instanceOf(FieldMapper.class));
        assertEquals("keyword", grandchildMapper.typeName());
    }

    public void testPropertiesFieldMultiChildMerge() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "properties": {
                  "properties": {
                    "child1": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword"
                        }
                      }
                    },
                    "child2": {
                      "type": "text"
                    },
                    "child3": {
                      "properties": {
                        "grandchild": {
                          "type": "text"
                        }
                      }
                    }
                  }
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "properties": {
                  "properties": {
                    "child2": {
                      "type": "integer"
                    },
                    "child3": {
                      "properties": {
                        "grandchild": {
                          "type": "long"
                        }
                      }
                    }
                  }
                }
              }
            }""");

        MapperService mapperService = createMapperService(mapping(b -> {}));
        mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE);
        assertEquals("""
            {
              "_doc" : {
                "properties" : {
                  "properties" : {
                    "properties" : {
                      "child1" : {
                        "type" : "text",
                        "fields" : {
                          "keyword" : {
                            "type" : "keyword"
                          }
                        }
                      },
                      "child2" : {
                        "type" : "integer"
                      },
                      "child3" : {
                        "properties" : {
                          "grandchild" : {
                            "type" : "long"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }""", Strings.toString(mapperService.documentMapper().mapping(), true, true));

        Mapper propertiesMapper = mapperService.documentMapper().mapping().getRoot().getMapper("properties");
        assertThat(propertiesMapper, instanceOf(ObjectMapper.class));
        Mapper childMapper = ((ObjectMapper) propertiesMapper).getMapper("child1");
        assertThat(childMapper, instanceOf(FieldMapper.class));
        assertEquals("text", childMapper.typeName());
        assertEquals(2, childMapper.getTotalFieldsCount());
        childMapper = ((ObjectMapper) propertiesMapper).getMapper("child2");
        assertThat(childMapper, instanceOf(FieldMapper.class));
        assertEquals("integer", childMapper.typeName());
        assertEquals(1, childMapper.getTotalFieldsCount());
        childMapper = ((ObjectMapper) propertiesMapper).getMapper("child3");
        assertThat(childMapper, instanceOf(ObjectMapper.class));
        Mapper grandchildMapper = ((ObjectMapper) childMapper).getMapper("grandchild");
        assertEquals("long", grandchildMapper.typeName());
    }

    public void testMergeUntilLimit() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "parent.child1": {
                  "type": "keyword"
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "parent.child2": {
                  "type": "keyword"
                }
              }
            }""");

        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 2)
            .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
            .build();

        final MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        DocumentMapper mapper = mapperService.merge("_doc", mapping1, MergeReason.MAPPING_AUTO_UPDATE);
        mapper = mapperService.merge("_doc", mapping2, MergeReason.MAPPING_AUTO_UPDATE);
        assertNotNull(mapper.mappers().getMapper("parent.child1"));
        assertNull(mapper.mappers().getMapper("parent.child2"));
    }

    public void testMergeUntilLimitMixedObjectAndDottedNotation() throws IOException {
        CompressedXContent mapping = new CompressedXContent("""
            {
              "properties": {
                "parent": {
                  "properties": {
                    "child1": {
                      "type": "keyword"
                    }
                  }
                },
                "parent.child2": {
                  "type": "keyword"
                }
              }
            }""");

        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 2)
            .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
            .build();

        final MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        DocumentMapper mapper = mapperService.merge("_doc", mapping, MergeReason.MAPPING_AUTO_UPDATE);
        assertEquals(0, mapper.mappers().remainingFieldsUntilLimit(2));
        assertNotNull(mapper.mappers().objectMappers().get("parent"));
        // the order is not deterministic, but we expect one to be null and the other to be non-null
        assertTrue(mapper.mappers().getMapper("parent.child1") == null ^ mapper.mappers().getMapper("parent.child2") == null);
    }

    public void testUpdateMappingWhenAtLimit() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "parent.child1": {
                  "type": "boolean"
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "parent.child1": {
                  "type": "boolean",
                  "ignore_malformed": true
                }
              }
            }""");

        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 2)
            .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
            .build();

        final MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        DocumentMapper mapper = mapperService.merge("_doc", mapping1, MergeReason.MAPPING_AUTO_UPDATE);
        mapper = mapperService.merge("_doc", mapping2, MergeReason.MAPPING_AUTO_UPDATE);
        assertNotNull(mapper.mappers().getMapper("parent.child1"));
        assertTrue(((BooleanFieldMapper) mapper.mappers().getMapper("parent.child1")).ignoreMalformed());
    }

    public void testMultiFieldsUpdate() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "text_field": {
                  "type": "text",
                  "fields": {
                    "multi_field1": {
                      "type":  "boolean"
                    }
                  }
                }
              }
            }""");

        // changes a mapping parameter for multi_field1 and adds another multi field which is supposed to be ignored
        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "text_field": {
                  "type": "text",
                  "fields": {
                    "multi_field1": {
                      "type":  "boolean",
                      "ignore_malformed": true
                    },
                    "multi_field2": {
                      "type":  "keyword"
                    }
                  }
                }
              }
            }""");

        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 2)
            .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
            .build();

        final MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        DocumentMapper mapper = mapperService.merge("_doc", mapping1, MergeReason.MAPPING_AUTO_UPDATE);
        mapper = mapperService.merge("_doc", mapping2, MergeReason.MAPPING_AUTO_UPDATE);
        assertNotNull(mapper.mappers().getMapper("text_field"));
        FieldMapper.MultiFields multiFields = ((TextFieldMapper) mapper.mappers().getMapper("text_field")).multiFields();
        Map<String, FieldMapper> multiFieldMap = StreamSupport.stream(multiFields.spliterator(), false)
            .collect(Collectors.toMap(FieldMapper::fullPath, Function.identity()));
        assertThat(multiFieldMap.keySet(), contains("text_field.multi_field1"));
        assertTrue(multiFieldMap.get("text_field.multi_field1").ignoreMalformed());
    }

    public void testMultiFieldExceedsLimit() throws IOException {
        CompressedXContent mapping = new CompressedXContent("""
            {
              "properties": {
                "multi_field": {
                  "type": "text",
                  "fields": {
                    "multi_field1": {
                      "type":  "boolean"
                    }
                  }
                },
                "keyword_field": {
                  "type": "keyword"
                }
              }
            }""");

        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1)
            .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
            .build();

        final MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        DocumentMapper mapper = mapperService.merge("_doc", mapping, MergeReason.MAPPING_AUTO_UPDATE);
        assertNull(mapper.mappers().getMapper("multi_field"));
        assertNotNull(mapper.mappers().getMapper("keyword_field"));
    }

    public void testMergeUntilLimitInitialMappingExceedsLimit() throws IOException {
        CompressedXContent mapping = new CompressedXContent("""
            {
              "properties": {
                "field1": {
                  "type": "keyword"
                },
                "field2": {
                  "type": "keyword"
                }
              }
            }""");

        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1)
            .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
            .build();

        final MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        DocumentMapper mapper = mapperService.merge("_doc", mapping, MergeReason.MAPPING_AUTO_UPDATE);
        // the order is not deterministic, but we expect one to be null and the other to be non-null
        assertTrue(mapper.mappers().getMapper("field1") == null ^ mapper.mappers().getMapper("field2") == null);
    }

    public void testMergeUntilLimitCapacityOnlyForParent() throws IOException {
        CompressedXContent mapping = new CompressedXContent("""
            {
              "properties": {
                "parent.child": {
                  "type": "keyword"
                }
              }
            }""");

        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 1)
            .put(INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), true)
            .build();

        final MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        DocumentMapper mapper = mapperService.merge("_doc", mapping, MergeReason.MAPPING_AUTO_UPDATE);
        assertNotNull(mapper.mappers().objectMappers().get("parent"));
        assertNull(mapper.mappers().getMapper("parent.child"));
    }

    public void testAutoFlattenObjectsSubobjectsTopLevelMerge() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "subobjects": false
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "parent": {
                  "properties": {
                    "child": {
                    "dynamic": true,
                      "properties": {
                        "grandchild": {
                          "type": "keyword"
                        }
                      }
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "subobjects" : false,
                "properties" : {
                  "parent.child.grandchild" : {
                    "type" : "keyword"
                  }
                }
              }
            }""");
    }

    public void testAutoFlattenObjectsSubobjectsMerge() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties" : {
                "parent" : {
                  "properties" : {
                    "child" : {
                      "type": "object"
                    }
                  }
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties" : {
                "parent" : {
                  "subobjects" : false,
                  "properties" : {
                    "child" : {
                      "properties" : {
                        "grandchild" : {
                          "type" : "keyword"
                        }
                      }
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "subobjects" : false,
                    "properties" : {
                      "child.grandchild" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping2, mapping1), """
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "subobjects" : false,
                    "properties" : {
                      "child.grandchild" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testAutoFlattenObjectsSubobjectsMergeConflictingMappingParameter() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "subobjects": false
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "parent": {
                  "dynamic": "false",
                  "properties": {
                    "child": {
                      "properties": {
                        "grandchild": {
                          "type": "keyword"
                        }
                      }
                    }
                  }
                }
              }
            }""");

        final MapperService mapperService = createMapperService(mapping(b -> {}));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Failed to parse mapping: Object mapper [parent] was found in a context where subobjects is set to false. "
                    + "Auto-flattening [parent] failed because the value of [dynamic] (FALSE) is not compatible "
                    + "with the value from its parent context (TRUE)"
            )
        );
    }

    public void testAutoFlattenObjectsSubobjectsMergeConflictingMappingParameterRoot() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "subobjects": false,
              "dynamic": false
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "subobjects": false,
              "properties": {
                "parent": {
                  "dynamic": "true",
                  "properties": {
                    "child": {
                      "properties": {
                        "grandchild": {
                          "type": "keyword"
                        }
                      }
                    }
                  }
                }
              }
            }""");

        final MapperService mapperService = createMapperService(mapping(b -> {}));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapperService.merge("_doc", List.of(mapping1, mapping2), MergeReason.INDEX_TEMPLATE)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Failed to parse mapping: Object mapper [parent] was found in a context where subobjects is set to false. "
                    + "Auto-flattening [parent] failed because the value of [dynamic] (TRUE) is not compatible "
                    + "with the value from its parent context (FALSE)"
            )
        );
    }

    public void testAutoFlattenObjectsSubobjectsMergeNonConflictingMappingParameter() throws IOException {
        CompressedXContent mapping = new CompressedXContent("""
            {
              "dynamic": false,
              "properties": {
                "parent": {
                  "dynamic": true,
                  "enabled": false,
                  "subobjects": false,
                  "properties": {
                    "child": {
                      "properties": {
                        "grandchild": {
                          "type": "keyword"
                        }
                      }
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping), """
            {
              "_doc" : {
                "dynamic" : "false",
                "properties" : {
                  "parent" : {
                    "dynamic" : "true",
                    "enabled" : false,
                    "subobjects" : false,
                    "properties" : {
                      "child.grandchild" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testExpandDottedNotationToObjectMappers() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "parent.child": {
                  "type": "keyword"
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("{}");

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "properties" : {
                      "child" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testMergeDottedAndNestedNotation() throws IOException {
        CompressedXContent mapping1 = new CompressedXContent("""
            {
              "properties": {
                "parent.child": {
                  "type": "keyword"
                }
              }
            }""");

        CompressedXContent mapping2 = new CompressedXContent("""
            {
              "properties": {
                "parent" : {
                  "properties" : {
                    "child" : {
                      "type" : "integer"
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping1, mapping2), """
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "properties" : {
                      "child" : {
                        "type" : "integer"
                      }
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping2, mapping1), """
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "properties" : {
                      "child" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""");
    }

    public void testDottedAndNestedNotationInSameMapping() throws IOException {
        CompressedXContent mapping = new CompressedXContent("""
            {
              "properties": {
                "parent.child": {
                  "type": "keyword"
                },
                "parent" : {
                  "properties" : {
                    "child" : {
                      "type" : "integer"
                    }
                  }
                }
              }
            }""");

        assertMergeEquals(List.of(mapping), """
            {
              "_doc" : {
                "properties" : {
                  "parent" : {
                    "properties" : {
                      "child" : {
                        "type" : "integer"
                      }
                    }
                  }
                }
              }
            }""");
    }

    private void assertMergeEquals(List<CompressedXContent> mappingSources, String expected) throws IOException {
        final MapperService mapperServiceBulk = createMapperService(mapping(b -> {}));
        // simulates multiple component templates being merged in a composable index template
        mapperServiceBulk.merge("_doc", mappingSources, MergeReason.INDEX_TEMPLATE);
        assertEquals(expected, Strings.toString(mapperServiceBulk.documentMapper().mapping(), true, true));

        MapperService mapperServiceSequential = createMapperService(mapping(b -> {}));
        // simulates a series of mapping updates
        mappingSources.forEach(m -> mapperServiceSequential.merge("_doc", m, MergeReason.INDEX_TEMPLATE));
        assertEquals(expected, Strings.toString(mapperServiceSequential.documentMapper().mapping(), true, true));
    }
}
