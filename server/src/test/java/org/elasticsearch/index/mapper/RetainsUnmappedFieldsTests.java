/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests {@link RootObjectMapper#canRetainUnmappedFields()}, which tells whether a mapping keeps the fields it does not declare,
 * and {@link UnmappedFields#mayExist()}, which combines it with what the index recorded of its earlier mappings, with the
 * index mode and with what the segments of a shard hold.
 */
public class RetainsUnmappedFieldsTests extends MapperServiceTestCase {

    public void testNotRetainedWhenEveryFieldIsMappedOrRejected() throws IOException {
        for (String dynamic : new String[] { null, "true", "strict" }) {
            MapperService mapperService = createMapperService(topMapping(b -> {
                if (dynamic != null) {
                    b.field("dynamic", dynamic);
                }
                b.startObject("properties");
                b.startObject("field").field("type", "keyword").endObject();
                b.startObject("object").startObject("properties").startObject("leaf").field("type", "long").endObject().endObject();
                b.endObject();
                b.endObject();
            }));
            assertRetains(mapperService, false);
        }
    }

    public void testRetainedByRootDynamic() throws IOException {
        assertRetains(createMapperService(topMapping(b -> b.field("dynamic", false))), true);
    }

    /**
     * A runtime field can be removed, which leaves the values documents hold for it without a field.
     */
    public void testRetainedByRuntimeFields() throws IOException {
        assertRetains(createMapperService(topMapping(b -> b.field("dynamic", "runtime"))), true);
        assertRetains(
            createMapperService(mapping(b -> b.startObject("object").field("type", "object").field("dynamic", "runtime").endObject())),
            true
        );
        assertRetains(
            createMapperService(
                topMapping(b -> b.startObject("runtime").startObject("field").field("type", "keyword").endObject().endObject())
            ),
            true
        );
    }

    public void testRetainedByDisabledRoot() throws IOException {
        assertRetains(createMapperService(topMapping(b -> b.field("enabled", false))), true);
    }

    public void testRetainedByObjectBelowRoot() throws IOException {
        assertRetains(createMapperService(mapping(b -> {
            b.startObject("outer").startObject("properties");
            b.startObject("inner").field("type", "object").field("dynamic", false).endObject();
            b.endObject().endObject();
        })), true);
        assertRetains(createMapperService(mapping(b -> {
            b.startObject("outer").startObject("properties");
            b.startObject("inner").field("type", "object").field("enabled", false).endObject();
            b.endObject().endObject();
        })), true);
    }

    /**
     * The mapping only answers for itself: what its earlier versions allowed is recorded by the index.
     */
    public void testFollowsTheMapping() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        assertRetains(mapperService, true);

        merge(mapperService, topMapping(b -> b.field("dynamic", randomFrom("true", "strict"))));
        assertRetains(mapperService, false);

        merge(mapperService, mapping(b -> b.startObject("object").field("type", "object").field("enabled", false).endObject()));
        assertRetains(mapperService, true);
    }

    public void testIsNotAMappingParameter() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(topMapping(b -> b.field("retains_unmapped_fields", true)))
        );
        assertThat(e.getMessage(), containsString("unsupported parameters"));
        assertThat(e.getMessage(), containsString("retains_unmapped_fields"));
    }

    /**
     * Documents indexed while a mapping kept unmapped fields still hold them after the mapping is tightened, which the index
     * records.
     */
    public void testStoredSourceWithEarlierMappingThatRetained() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.RETAINS_UNMAPPED_FIELDS_SETTING.getKey(), true).build();
        MapperService mapperService = createMapperService(
            settings,
            mapping(b -> b.startObject("field").field("type", "keyword").endObject())
        );
        assertRetains(mapperService, false);
        assertMayHoldUnmappedFields(mapperService, b -> b.field("field", "value"), true);
    }

    /**
     * An index created before its mappings were tracked has an unknown history, so it reads as retaining.
     */
    public void testUntrackedBeforeIndexVersion() throws IOException {
        IndexVersion before = IndexVersionUtils.getPreviousVersion(IndexVersions.TRACKS_UNMAPPED_FIELD_RETENTION);
        assertTrue(IndexSettings.retainsUnmappedFields(before, Settings.EMPTY));
        assertFalse(IndexSettings.retainsUnmappedFields(IndexVersions.TRACKS_UNMAPPED_FIELD_RETENTION, Settings.EMPTY));

        MapperService mapperService = createMapperService(
            before,
            mapping(b -> b.startObject("field").field("type", "keyword").endObject())
        );
        assertRetains(mapperService, false);
        assertTrue(mapperService.getIndexSettings().retainsUnmappedFields());
        assertMayHoldUnmappedFields(mapperService, b -> b.field("field", "value"), true);
    }

    public void testStoredSourceWithoutUnmappedFields() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("field").field("type", "keyword").endObject()));
        assertMayHoldUnmappedFields(mapperService, b -> b.field("field", "value").field("dynamically_mapped", "value"), false);
    }

    public void testStoredSourceWithMappingThatRetains() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        assertMayHoldUnmappedFields(mapperService, b -> b.field("unmapped", "value"), true);
    }

    /**
     * A dynamic field skipped for exceeding the field limit stays in {@code _source} without the mapping having allowed it.
     */
    public void testStoredSourceWithDynamicFieldBeyondLimit() throws IOException {
        Settings settings = Settings.builder()
            .put("index.mapping.total_fields.limit", 1)
            .put("index.mapping.total_fields.ignore_dynamic_beyond_limit", true)
            .build();
        MapperService mapperService = createMapperService(
            settings,
            mapping(b -> b.startObject("field").field("type", "keyword").endObject())
        );
        assertRetains(mapperService, false);
        assertMayHoldUnmappedFields(mapperService, b -> b.field("field", "value"), false);
        assertMayHoldUnmappedFields(mapperService, b -> b.field("field", "value").field("beyond_limit", "value"), true);
    }

    public void testSyntheticSource() throws IOException {
        Settings settings = Settings.builder().put("index.mapping.source.mode", "synthetic").build();
        MapperService mapperService = createMapperService(settings, topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties").startObject("field").field("type", "keyword").endObject().endObject();
        }));
        assertRetains(mapperService, true);
        // The mapping would keep an unmapped field, but no document holds one.
        assertMayHoldUnmappedFields(mapperService, b -> b.field("field", "value"), false);
        assertMayHoldUnmappedFields(mapperService, b -> b.field("field", "value").field("unmapped", "value"), true);
    }

    public void testStrictColumnarDropsUnmappedFields() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        MapperService mapperService = createMapperService(settings, topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties").startObject("field").field("type", "keyword").endObject().endObject();
        }));
        assertFalse(createSearchExecutionContext(mapperService).unmappedFields().mayExist());
        assertMayHoldUnmappedFields(mapperService, b -> b.field("field", "value").field("unmapped", "value"), false);
    }

    public void testWithoutSearcher() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("field").field("type", "keyword").endObject()));
        assertRetains(mapperService, false);
        assertTrue(createSearchExecutionContext(mapperService).unmappedFields().mayExist());
    }

    private static void assertRetains(MapperService mapperService, boolean expected) {
        assertThat(mapperService.mappingLookup().getMapping().getRoot().canRetainUnmappedFields(), equalTo(expected));
        assertThat(mapperService.documentMapper().mappingSource().string(), not(containsString("retains_unmapped_fields")));
    }

    private void assertMayHoldUnmappedFields(
        MapperService mapperService,
        CheckedConsumer<XContentBuilder, IOException> document,
        boolean expected
    ) throws IOException {
        ParsedDocument parsed = mapperService.documentMapper().parse(source(document));
        if (parsed.dynamicMappingsUpdate() != null) {
            merge(mapperService, MapperService.MergeReason.MAPPING_UPDATE, parsed.dynamicMappingsUpdate().string());
            parsed = mapperService.documentMapper().parse(source(document));
        }
        ParsedDocument toIndex = parsed;
        withLuceneIndex(mapperService, writer -> writer.addDocuments(toIndex.docs()), reader -> {
            SearchExecutionContext context = createSearchExecutionContext(mapperService, newSearcher(reader));
            UnmappedFields unmappedFields = context.unmappedFields();
            assertThat(unmappedFields.mayExist(), equalTo(expected));
            // Nothing is read, by name or by walking, from a shard that holds no unmapped field.
            assertThat(unmappedFields.mayExist("not_in_the_mapping"), equalTo(expected));
            assertThat(unmappedFields.blockLoader("not_in_the_mapping") == ConstantNull.INSTANCE, equalTo(expected == false));
            BlockLoader all = unmappedFields.allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 1.0));
            assertThat(all == ConstantNull.INSTANCE, equalTo(expected == false));
        });
    }
}
