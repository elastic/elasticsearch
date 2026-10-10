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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests the {@code retains_unmapped_fields} property a mapping records about itself, and
 * {@link SearchExecutionContext#mayHoldUnmappedFields()}, which combines it with the index mode and with what the segments of a
 * shard hold.
 */
public class RetainsUnmappedFieldsTests extends MapperServiceTestCase {

    private static final String PROPERTY = "\"" + RootObjectMapper.RETAINS_UNMAPPED_FIELDS + "\"";

    public void testNotRetainedWhenEveryFieldIsMappedOrRejected() throws IOException {
        for (String dynamic : new String[] { null, "true", "runtime", "strict" }) {
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
     * Documents indexed while the mapping kept unmapped fields still hold them after the mapping is tightened.
     */
    public void testStaysRetainedOnceTheMappingIsTightened() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        assertRetains(mapperService, true);

        merge(mapperService, topMapping(b -> b.field("dynamic", randomFrom("true", "strict"))));
        assertThat(mapperService.mappingLookup().getMapping().getRoot().dynamic(), not(equalTo(ObjectMapper.Dynamic.FALSE)));
        assertRetains(mapperService, true);

        merge(mapperService, mapping(b -> b.startObject("field").field("type", "keyword").endObject()));
        assertRetains(mapperService, true);
    }

    public void testBecomesRetainedWhenAnUpdateLoosensTheMapping() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("field").field("type", "keyword").endObject()));
        assertRetains(mapperService, false);

        merge(mapperService, mapping(b -> b.startObject("object").field("type", "object").field("enabled", false).endObject()));
        assertRetains(mapperService, true);
    }

    public void testCannotBeClearedExplicitly() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        merge(mapperService, topMapping(b -> b.field("dynamic", true).field(RootObjectMapper.RETAINS_UNMAPPED_FIELDS, false)));
        assertRetains(mapperService, true);
    }

    public void testCanBeSetExplicitly() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field(RootObjectMapper.RETAINS_UNMAPPED_FIELDS, true)));
        assertRetains(mapperService, true);
    }

    /**
     * An index created before the property existed has an unknown history, so it reads as retaining without recording so.
     */
    public void testUntrackedBeforeIndexVersion() throws IOException {
        IndexVersion before = IndexVersionUtils.getPreviousVersion(IndexVersions.MAPPING_TRACKS_UNMAPPED_FIELD_RETENTION);
        for (String dynamic : new String[] { "true", "false" }) {
            MapperService mapperService = createMapperService(before, topMapping(b -> b.field("dynamic", dynamic)));
            assertTrue(mapperService.mappingLookup().getMapping().getRoot().retainsUnmappedFields());
            assertThat(mapperService.documentMapper().mappingSource().string(), not(containsString(PROPERTY)));
        }
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
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
        MapperService mapperService = createMapperService(settings, topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties").startObject("field").field("type", "keyword").endObject().endObject();
        }));
        assertFalse(createSearchExecutionContext(mapperService).mayHoldUnmappedFields());
        assertMayHoldUnmappedFields(mapperService, b -> b.field("field", "value").field("unmapped", "value"), false);
    }

    public void testWithoutSearcher() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("field").field("type", "keyword").endObject()));
        assertRetains(mapperService, false);
        assertTrue(createSearchExecutionContext(mapperService).mayHoldUnmappedFields());
    }

    private static void assertRetains(MapperService mapperService, boolean expected) {
        assertThat(mapperService.mappingLookup().getMapping().getRoot().retainsUnmappedFields(), equalTo(expected));
        String source = mapperService.documentMapper().mappingSource().string();
        assertThat(source, expected ? containsString(PROPERTY + ":true") : not(containsString(PROPERTY)));
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
            assertThat(context.mayHoldUnmappedFields(), equalTo(expected));
        });
    }
}
