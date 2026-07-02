/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SourceFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return SourceFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return true;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "enabled",
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", false).endObject()),
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", true).endObject()),
            dm -> {}
        );
        checker.registerUpdateCheck(
            "enabled",
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", true).endObject()),
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", false).endObject()),
            dm -> assertFalse(dm.metadataMapper(SourceFieldMapper.class).enabled())
        );
        checker.registerUpdateCheck(
            "mode",
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("mode", "stored").endObject()),
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("mode", "synthetic").endObject()),
            dm -> {}
        );
        // mode is controlled by index settings on current index versions rather than serialized in the mapping
        checker.excludeFromSerialization("mode");
        checker.registerConflictCheck("includes", b -> b.array("includes", "foo*"));
        checker.registerConflictCheck("excludes", b -> b.array("excludes", "foo*"));
    }

    public void testNoFormat() throws Exception {

        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> b.startObject("_source").endObject()));
        ParsedDocument doc = documentMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject()),
                XContentType.JSON
            )
        );

        assertThat(XContentHelper.xContentType(doc.source().originalBytes()), equalTo(XContentType.JSON));

        doc = documentMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.smileBuilder().startObject().field("field", "value").endObject()),
                XContentType.SMILE
            )
        );

        assertThat(XContentHelper.xContentType(doc.source().originalBytes()), equalTo(XContentType.SMILE));
    }

    public void testIncludes() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(
            topMapping(b -> b.startObject("_source").array("includes", "path1*").endObject())
        );

        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.startObject("path1").field("field1", "value1").endObject();
            b.startObject("path2").field("field2", "value2").endObject();
        }));

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        assertThat(sourceAsMap.containsKey("path1"), equalTo(true));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(false));
    }

    public void testDuplicatedIncludes() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(
            topMapping(b -> b.startObject("_source").array("includes", "path1", "path1").endObject())
        );

        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.startObject("path1").field("field1", "value1").endObject();
            b.startObject("path2").field("field2", "value2").endObject();
        }));

        IndexableField sourceField = doc.rootDoc().getField("_source");
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            assertEquals(Map.of("path1", Map.of("field1", "value1")), parser.map());
        }
    }

    public void testExcludes() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(
            topMapping(b -> b.startObject("_source").array("excludes", "path1*").endObject())
        );

        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.startObject("path1").field("field1", "value1").endObject();
            b.startObject("path2").field("field2", "value2").endObject();
        }));

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        assertThat(sourceAsMap.containsKey("path1"), equalTo(false));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(true));
    }

    public void testDuplicatedExcludes() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(
            topMapping(b -> b.startObject("_source").array("excludes", "path1", "path1").endObject())
        );

        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.startObject("path1").field("field1", "value1").endObject();
            b.startObject("path2").field("field2", "value2").endObject();
        }));

        IndexableField sourceField = doc.rootDoc().getField("_source");
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            assertEquals(Map.of("path2", Map.of("field2", "value2")), parser.map());
        }
    }

    public void testComplete() throws Exception {

        assertTrue(createDocumentMapper(topMapping(b -> {})).sourceMapper().isComplete());

        assertFalse(
            createDocumentMapper(topMapping(b -> b.startObject("_source").field("enabled", false).endObject())).sourceMapper().isComplete()
        );

        assertFalse(
            createDocumentMapper(topMapping(b -> b.startObject("_source").array("includes", "foo*").endObject())).sourceMapper()
                .isComplete()
        );

        assertFalse(
            createDocumentMapper(topMapping(b -> b.startObject("_source").array("excludes", "foo*").endObject())).sourceMapper()
                .isComplete()
        );
    }

    public void testSourceObjectContainsExtraTokens() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> {}));

        Exception exception = expectThrows(
            DocumentParsingException.class,
            // extra end object (invalid JSON))
            () -> documentMapper.parse(new SourceToParse("1", new BytesArray("{}}"), XContentType.JSON))
        );
        assertNotNull(exception.getCause());
        assertThat(exception.getCause().getMessage(), containsString("Unexpected close marker '}'"));
    }

    public void testSyntheticDisabledNotSupported() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                topMapping(b -> b.startObject("_source").field("enabled", false).field("mode", "synthetic").endObject())
            )
        );
        assertThat(e.getMessage(), containsString("Cannot set both [mode] and [enabled] parameters"));
    }

    public void testSyntheticUpdates() throws Exception {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "_source" : { "mode" : "synthetic" } } }
            """);
        SourceFieldMapper mapper = mapperService.documentMapper().sourceMapper();
        assertTrue(mapper.enabled());
        assertFalse("mode is a noop parameter", mapper.isSynthetic());

        merge(mapperService, """
            { "_doc" : { "_source" : { "mode" : "synthetic" } } }
            """);
        mapper = mapperService.documentMapper().sourceMapper();
        assertTrue(mapper.enabled());
        assertFalse("mode is a noop parameter", mapper.isSynthetic());

        ParsedDocument doc = mapperService.documentMapper().parse(source("{}"));
        assertNull(doc.rootDoc().get(SourceFieldMapper.NAME));

        merge(mapperService, """
            { "_doc" : { "_source" : { "mode" : "disabled" } } }
            """);

        mapper = mapperService.documentMapper().sourceMapper();
        assertTrue("mode is a noop parameter", mapper.enabled());
        assertFalse("mode is a noop parameter", mapper.isSynthetic());
    }

    public void testSyntheticUpdatesLegacy() throws Exception {
        var mappings = XContentBuilder.builder(XContentType.JSON.xContent()).startObject().startObject("_doc").startObject("_source");
        mappings.field("mode", "synthetic").endObject().endObject().endObject();
        var version = IndexVersionUtils.getPreviousVersion(IndexVersions.SOURCE_MAPPER_MODE_ATTRIBUTE_NOOP);
        MapperService mapperService = createMapperService(version, mappings);
        SourceFieldMapper mapper = mapperService.documentMapper().sourceMapper();
        assertTrue(mapper.enabled());
        assertTrue(mapper.isSynthetic());

        merge(mapperService, """
            { "_doc" : { "_source" : { "mode" : "synthetic" } } }
            """);
        mapper = mapperService.documentMapper().sourceMapper();
        assertTrue(mapper.enabled());
        assertTrue(mapper.isSynthetic());

        ParsedDocument doc = mapperService.documentMapper().parse(source("{}"));
        assertNull(doc.rootDoc().get(SourceFieldMapper.NAME));

        merge(mapperService, """
            { "_doc" : { "_source" : { "mode" : "disabled" } } }
            """);

        mapper = mapperService.documentMapper().sourceMapper();
        assertFalse(mapper.enabled());
        assertFalse(mapper.isSynthetic());
    }

    public void testSyntheticSourceInTimeSeries() throws IOException {
        XContentBuilder mapping = fieldMapping(b -> {
            b.field("type", "keyword");
            b.field("time_series_dimension", true);
        });
        DocumentMapper mapper = createTimeSeriesModeDocumentMapper(mapping);
        assertTrue(mapper.sourceMapper().isSynthetic());
        assertEquals("{}", mapper.sourceMapper().toString());
    }

    public void testSyntheticSourceWithLogsIndexMode() throws IOException {
        XContentBuilder mapping = fieldMapping(b -> { b.field("type", "keyword"); });
        DocumentMapper mapper = createLogsModeDocumentMapper(mapping);
        assertTrue(mapper.sourceMapper().isSynthetic());
        assertEquals("{}", mapper.sourceMapper().toString());
    }

    public void testSyntheticSourceWithColumnarIndexMode() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        XContentBuilder mapping = fieldMapping(b -> { b.field("type", "keyword"); });
        DocumentMapper mapper = createColumnarModeDocumentMapper(mapping);
        assertTrue(mapper.sourceMapper().isSynthetic());
        assertEquals("{}", mapper.sourceMapper().toString());
    }

    public void testSyntheticSourceWithColumnarLogsdbIndexMode() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        XContentBuilder mapping = fieldMapping(b -> { b.field("type", "keyword"); });
        DocumentMapper mapper = createColumnarLogsdbModeDocumentMapper(mapping);
        assertTrue(mapper.sourceMapper().isSynthetic());
        assertEquals("{}", mapper.sourceMapper().toString());
    }

    public void testSupportsNonDefaultParameterValues() throws IOException {
        Settings settings = Settings.builder().put(SourceFieldMapper.LOSSY_PARAMETERS_ALLOWED_SETTING_NAME, false).build();
        {
            var sourceFieldMapper = createMapperService(settings, topMapping(b -> b.startObject("_source").endObject())).documentMapper()
                .sourceMapper();
            assertThat(sourceFieldMapper, notNullValue());
        }
        {
            var sourceFieldMapper = createMapperService(
                settings,
                topMapping(b -> b.startObject("_source").field("mode", randomBoolean() ? "synthetic" : "stored").endObject())
            ).documentMapper().sourceMapper();
            assertThat(sourceFieldMapper, notNullValue());
        }
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(settings, topMapping(b -> b.startObject("_source").field("enabled", false).endObject()))
                .documentMapper()
                .sourceMapper()
        );
        assertThat(e.getMessage(), containsString("Parameter [enabled] is not allowed in source"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(settings, topMapping(b -> b.startObject("_source").array("includes", "foo").endObject()))
                .documentMapper()
                .sourceMapper()
        );
        assertThat(e.getMessage(), containsString("Parameter [includes] is not allowed in source"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(settings, topMapping(b -> b.startObject("_source").array("excludes", "foo").endObject()))
                .documentMapper()
                .sourceMapper()
        );
        assertThat(e.getMessage(), containsString("Parameter [excludes] is not allowed in source"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(settings, topMapping(b -> b.startObject("_source").field("mode", "disabled").endObject()))
                .documentMapper()
                .sourceMapper()
        );
        assertThat(e.getMessage(), containsString("Parameter [mode=disabled] is not allowed in source"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                settings,
                topMapping(
                    b -> b.startObject("_source").field("enabled", false).array("includes", "foo").array("excludes", "foo").endObject()
                )
            ).documentMapper().sourceMapper()
        );
        assertThat(e.getMessage(), containsString("Parameters [enabled,includes,excludes] are not allowed in source"));
    }

    public void testBypassCheckForNonDefaultParameterValuesInEarlierVersions() throws IOException {
        Settings settings = Settings.builder().put(SourceFieldMapper.LOSSY_PARAMETERS_ALLOWED_SETTING_NAME, false).build();
        {
            var sourceFieldMapper = createMapperService(
                IndexVersionUtils.getPreviousVersion(IndexVersions.SOURCE_MAPPER_LOSSY_PARAMS_CHECK),
                settings,
                () -> true,
                topMapping(b -> b.startObject("_source").field("enabled", false).endObject())
            ).documentMapper().sourceMapper();
            assertThat(sourceFieldMapper, notNullValue());
        }
        {
            var sourceFieldMapper = createMapperService(
                IndexVersionUtils.getPreviousVersion(IndexVersions.SOURCE_MAPPER_LOSSY_PARAMS_CHECK),
                settings,
                () -> true,
                topMapping(b -> b.startObject("_source").array("includes", "foo").endObject())
            ).documentMapper().sourceMapper();
            assertThat(sourceFieldMapper, notNullValue());
        }
        {
            var sourceFieldMapper = createMapperService(
                IndexVersionUtils.getPreviousVersion(IndexVersions.SOURCE_MAPPER_LOSSY_PARAMS_CHECK),
                settings,
                () -> true,
                topMapping(b -> b.startObject("_source").array("excludes", "foo").endObject())
            ).documentMapper().sourceMapper();
            assertThat(sourceFieldMapper, notNullValue());
        }
        {
            var sourceFieldMapper = createMapperService(
                IndexVersionUtils.getPreviousVersion(IndexVersions.SOURCE_MAPPER_LOSSY_PARAMS_CHECK),
                settings,
                () -> true,
                topMapping(b -> b.startObject("_source").field("mode", "disabled").endObject())
            ).documentMapper().sourceMapper();
            assertThat(sourceFieldMapper, notNullValue());
        }
    }

    public void testRecoverySourceWithSourceExcludes() throws IOException {
        {
            MapperService mapperService = createMapperService(
                topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("excludes", List.of("field1")).endObject())
            );
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> { b.field("field1", "value1"); }));

            assertNotNull(doc.rootDoc().getField("_recovery_source"));
            assertThat(doc.rootDoc().getField("_recovery_source").binaryValue(), equalTo(new BytesRef("{\"field1\":\"value1\"}")));
        }
        {
            Settings settings = Settings.builder().put(INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false).build();
            MapperService mapperService = createMapperService(
                settings,
                topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", false).endObject())
            );
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> b.field("field1", "value1")));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
    }

    public void testRecoverySourceWithSourceDisabled() throws IOException {
        {
            MapperService mapperService = createMapperService(
                topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", false).endObject())
            );
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> { b.field("field1", "value1"); }));
            assertNotNull(doc.rootDoc().getField("_recovery_source"));
            assertThat(doc.rootDoc().getField("_recovery_source").binaryValue(), equalTo(new BytesRef("{\"field1\":\"value1\"}")));
        }
        {
            Settings settings = Settings.builder().put(INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false).build();
            MapperService mapperService = createMapperService(
                settings,
                topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", false).endObject())
            );
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> b.field("field1", "value1")));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
    }

    public void testRecoverySourceWitInvalidSettings() {
        {
            Settings settings = Settings.builder().put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true).build();
            IllegalArgumentException exc = expectThrows(
                IllegalArgumentException.class,
                () -> createMapperService(settings, topMapping(b -> {}))
            );
            assertThat(
                exc.getMessage(),
                containsString(
                    String.format(
                        Locale.ROOT,
                        "The setting [%s] is only permitted",
                        IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey()
                    )
                )
            );
        }

        {
            Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED.toString())
                .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
                .build();
            IllegalArgumentException exc = expectThrows(
                IllegalArgumentException.class,
                () -> createMapperService(settings, topMapping(b -> {}))
            );
            assertThat(
                exc.getMessage(),
                containsString(
                    String.format(
                        Locale.ROOT,
                        "The setting [%s] is only permitted",
                        IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey()
                    )
                )
            );
        }
        {
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.toString())
                .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
                .build();
            IllegalArgumentException exc = expectThrows(
                IllegalArgumentException.class,
                () -> createMapperService(settings, topMapping(b -> {}))
            );
            assertThat(
                exc.getMessage(),
                containsString(
                    String.format(
                        Locale.ROOT,
                        "The setting [%s] is only permitted",
                        IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey()
                    )
                )
            );
        }
    }

    public void testColumnarStoredModeRequiresColumnarIndex() {
        // COLUMNAR_STORED is rejected on non-columnar index modes
        for (var nonColumnarMode : new IndexMode[] { IndexMode.STANDARD, IndexMode.LOGSDB, IndexMode.TIME_SERIES }) {
            Settings.Builder builder = Settings.builder()
                .put(IndexSettings.MODE.getKey(), nonColumnarMode.toString())
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString());
            if (nonColumnarMode == IndexMode.TIME_SERIES) {
                // time_series requires a routing_path; provide one so its own validation passes and our source mode validator fires
                builder.putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim");
            }
            Settings settings = builder.build();
            IllegalArgumentException exc = expectThrows(
                IllegalArgumentException.class,
                () -> createMapperService(settings, topMapping(b -> {}))
            );
            assertThat(
                exc.getMessage(),
                containsString(
                    "unsupported source mode [COLUMNAR_STORED] for index mode ["
                        + nonColumnarMode
                        + "]; supported values: [DISABLED, STORED, SYNTHETIC]"
                )
            );
        }
    }

    public void testNonColumnarSourceModesRejectedInColumnarIndex() {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        // DISABLED and STORED are rejected on columnar index modes (SYNTHETIC is allowed)
        for (var columnarMode : new IndexMode[] { IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR }) {
            for (var unsupportedMode : new SourceFieldMapper.Mode[] { SourceFieldMapper.Mode.DISABLED, SourceFieldMapper.Mode.STORED }) {
                Settings settings = Settings.builder()
                    .put(IndexSettings.MODE.getKey(), columnarMode.toString())
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), unsupportedMode.toString())
                    .build();
                IllegalArgumentException exc = expectThrows(
                    IllegalArgumentException.class,
                    () -> createMapperService(settings, topMapping(b -> {}))
                );
                assertThat(
                    exc.getMessage(),
                    containsString(
                        "unsupported source mode ["
                            + unsupportedMode
                            + "] for index mode ["
                            + columnarMode
                            + "]; supported values: [SYNTHETIC, COLUMNAR_STORED]"
                    )
                );
            }
        }
    }

    public void testSyntheticRecoverySourceRequiredForColumnarIndex() {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        // Disabling synthetic recovery source is rejected for columnar index modes
        for (var columnarMode : new IndexMode[] { IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR }) {
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), columnarMode.toString())
                .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), false)
                .build();
            IllegalArgumentException exc = expectThrows(
                IllegalArgumentException.class,
                () -> createMapperService(settings, topMapping(b -> {}))
            );
            assertThat(
                exc.getMessage(),
                containsString(
                    "The setting ["
                        + IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey()
                        + "] must not be false when ["
                        + IndexSettings.MODE.getKey()
                        + "] is set to ["
                        + columnarMode.name()
                        + "]."
                )
            );
        }
    }

    public void testRecoverySourceWithSyntheticSource() throws IOException {
        {
            Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.toString())
                .build();
            MapperService mapperService = createMapperService(settings, topMapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> b.field("field1", "value1")));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
        {
            Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.toString())
                .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
                .build();
            MapperService mapperService = createMapperService(settings, topMapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> b.field("field1", "value1")));
            assertNotNull(doc.rootDoc().getField("_recovery_source_size"));
            assertThat(doc.rootDoc().getField("_recovery_source_size").numericValue(), equalTo(19L));
        }
        {
            Settings settings = Settings.builder().put(INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false).build();
            MapperService mapperService = createMapperService(
                settings,
                topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("mode", "synthetic").endObject())
            );
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> b.field("field1", "value1")));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
    }

    public void testRecoverySourceWithColumnarStoredSource() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("field1");
            b.field("type", "keyword");
            b.endObject();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument doc = docMapper.parse(source(b -> b.field("field1", "value1")));
        // columnar_stored requires synthetic recovery source, so only the size is stored (not the full source)
        assertNull(doc.rootDoc().getField("_recovery_source"));
        assertNotNull(doc.rootDoc().getField("_recovery_source_size"));
    }

    public void testColumnarStoredSourceWithSkipIgnoredSourceWrite() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
            .put(IgnoredSourceFieldMapper.SKIP_IGNORED_SOURCE_WRITE_SETTING.getKey(), true)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("field1");
            b.field("type", "keyword");
            b.endObject();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument doc = docMapper.parse(source(b -> b.field("field1", "value1")));
        // columnar_stored always writes its whole-document _ignored_source entry, even
        // when skip_ignored_source_write=true suppresses per-field entries
        assertNotNull(doc.rootDoc().getField(IgnoredSourceFieldMapper.NAME));
        // synthetic recovery source is required for columnar_stored, so only the size is stored
        assertNull(doc.rootDoc().getField("_recovery_source"));
        assertNotNull(doc.rootDoc().getField("_recovery_source_size"));
    }

    /**
     * Verifies that in columnar_stored mode, the source loader reads from the pre-computed _ignored_source blob
     * rather than reconstructing source from doc values.
     */
    public void testColumnarStoredSourceReadsFromIgnoredSourceBlob() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("kwd");
            b.field("type", "keyword");
            b.endObject();
        }));

        // Parse a document so postParse() stores {"kwd":"blob_value"} in the _ignored_source blob.
        ParsedDocument parsed = mapperService.documentMapper().parse(source(b -> b.field("kwd", "blob_value")));

        // Build a modified Lucene document: keep the _ignored_source blob but change kwd doc values to "docvalues_value".
        List<IndexableField> modified = new ArrayList<>();
        for (IndexableField field : parsed.rootDoc()) {
            if (field instanceof MultiValuedBinaryDocValuesField && field.name().equals("kwd")) {
                var replacement = new MultiValuedBinaryDocValuesField.SeparateCount(
                    "kwd",
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                );
                replacement.add(new BytesRef("docvalues_value"));
                modified.add(replacement);
            } else {
                modified.add(field);
            }
        }

        withLuceneIndex(mapperService, iw -> iw.addDocument(modified), reader -> {
            SourceLoader loader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP);
            for (LeafReaderContext leaf : reader.leaves()) {
                int[] docIds = IntStream.range(0, leaf.reader().maxDoc()).toArray();
                SourceLoader.Leaf sourceLeaf = loader.leaf(leaf.reader(), docIds);
                LeafStoredFieldLoader sfLoader = StoredFieldLoader.create(false, loader.requiredStoredFields()).getLoader(leaf, docIds);
                sfLoader.advanceTo(0);
                Source source = sourceLeaf.source(sfLoader, 0);
                // Source comes from the _ignored_source blob, not from kwd doc values
                assertThat(source.internalSourceRef().utf8ToString(), equalTo("{\"kwd\":\"blob_value\"}"));
            }
        });
    }

    /**
     * Verifies that in columnar_stored mode a nested field round-trips: the source blob is materialized at index time
     * from the in-memory document tree (children reconstructed by parent-pointer match), and read back from the blob.
     */
    public void testColumnarStoredSourceNestedRoundTrip() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("comments");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("message").field("type", "keyword").endObject();
                    b.startObject("votes").field("type", "long").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsed = mapperService.documentMapper().parse(source(b -> {
            b.startArray("comments");
            b.startObject().field("message", "first").field("votes", 3).endObject();
            b.startObject().field("message", "second").field("votes", 7).endObject();
            b.endArray();
        }));

        // addDocuments indexes the nested children followed by the root as a block; the root is the last doc.
        withLuceneIndex(mapperService, iw -> iw.addDocuments(parsed.docs()), unwrapped -> {
            // The nested source loader builds a parent bitset, which needs a shard-wrapped reader (as in production).
            DirectoryReader reader = wrapInMockESDirectoryReader(unwrapped);
            SourceLoader loader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP);
            LeafReaderContext leaf = reader.leaves().get(0);
            int rootDocId = parsed.docs().size() - 1;
            int[] docIds = IntStream.range(0, leaf.reader().maxDoc()).toArray();
            SourceLoader.Leaf sourceLeaf = loader.leaf(leaf.reader(), docIds);
            LeafStoredFieldLoader sfLoader = StoredFieldLoader.create(false, loader.requiredStoredFields()).getLoader(leaf, docIds);
            sfLoader.advanceTo(rootDocId);
            Source source = sourceLeaf.source(sfLoader, rootDocId);
            assertThat(
                source.internalSourceRef().utf8ToString(),
                equalTo("{\"comments\":[{\"message\":\"first\",\"votes\":3},{\"message\":\"second\",\"votes\":7}]}")
            );
        });
    }

    /**
     * Verifies that a multi-valued leaf inside a nested object preserves array order in columnar_stored mode too: the
     * offsets recorded per child document are reconstructed when the index-time blob is materialized.
     */
    public void testColumnarStoredSourceNestedLeafArrayRoundTrip() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("comments");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("stars").field("type", "long").endObject();
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsed = mapperService.documentMapper().parse(source(b -> {
            b.startArray("comments");
            b.startObject().array("stars", 50, 10, 30).endObject();
            b.startObject().array("stars", 20, 40).endObject();
            b.endArray();
        }));

        withLuceneIndex(mapperService, iw -> iw.addDocuments(parsed.docs()), unwrapped -> {
            DirectoryReader reader = wrapInMockESDirectoryReader(unwrapped);
            SourceLoader loader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP);
            LeafReaderContext leaf = reader.leaves().get(0);
            int rootDocId = parsed.docs().size() - 1;
            int[] docIds = IntStream.range(0, leaf.reader().maxDoc()).toArray();
            SourceLoader.Leaf sourceLeaf = loader.leaf(leaf.reader(), docIds);
            LeafStoredFieldLoader sfLoader = StoredFieldLoader.create(false, loader.requiredStoredFields()).getLoader(leaf, docIds);
            sfLoader.advanceTo(rootDocId);
            Source source = sourceLeaf.source(sfLoader, rootDocId);
            assertThat(source.internalSourceRef().utf8ToString(), equalTo("{\"comments\":[{\"stars\":[50,10,30]},{\"stars\":[20,40]}]}"));
        });
    }

    /**
     * Verifies that in columnar_stored mode, {@code postParse} removes the per-field fallback fields used only for
     * synthetic-source reconstruction once the whole-document blob has been written to {@code _ignored_source}.
     */
    public void testColumnarStoredPrunesFallbackFields() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("num");
            b.field("type", "integer");
            b.field("ignore_malformed", true);
            b.endObject();
            b.startObject("kwd");
            b.field("type", "keyword");
            b.field("ignore_above", 3);
            b.endObject();
        }));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("num", "not_a_number");
            b.field("kwd", "long_ignored_value");
        }));
        LuceneDocument rootDoc = doc.rootDoc();
        // Fallback fields for synthetic-source reconstruction must have been pruned
        assertNull("._ignore_malformed field should have been pruned", rootDoc.getField("num._ignore_malformed"));
        assertNull("._ignore_malformed.counts field should have been pruned", rootDoc.getField("num._ignore_malformed.counts"));
        assertNull("._original field should have been pruned", rootDoc.getField("kwd._original"));
        assertNull("._original.counts field should have been pruned", rootDoc.getField("kwd._original.counts"));
        // The whole-document _ignored_source blob and the queryable _ignored meta-field must still be present
        assertNotNull("_ignored_source blob must still be present", rootDoc.getField(IgnoredSourceFieldMapper.NAME));
        assertNotNull("_ignored meta-field must still be present", rootDoc.getField(IgnoredFieldMapper.NAME));
    }

    public void testRecoverySourceWithLogs() throws IOException {
        {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> { b.field("@timestamp", "2012-02-13"); }));
            assertNotNull(doc.rootDoc().getField("_recovery_source_size"));
            assertThat(doc.rootDoc().getField("_recovery_source_size").numericValue(), equalTo(27L));
        }
        {
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName())
                .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
                .build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> { b.field("@timestamp", "2012-02-13"); }));
            assertNotNull(doc.rootDoc().getField("_recovery_source_size"));
            assertThat(doc.rootDoc().getField("_recovery_source_size").numericValue(), equalTo(27L));
        }
        {
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName())
                .put(INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
                .build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> b.field("@timestamp", "2012-02-13")));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
    }

    public void testRecoverySourceWithColumnar() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> { b.field("@timestamp", "2012-02-13"); }));
            assertNotNull(doc.rootDoc().getField("_recovery_source_size"));
            assertThat(doc.rootDoc().getField("_recovery_source_size").numericValue(), equalTo(27L));
        }
        {
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
                .build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> { b.field("@timestamp", "2012-02-13"); }));
            assertNotNull(doc.rootDoc().getField("_recovery_source_size"));
            assertThat(doc.rootDoc().getField("_recovery_source_size").numericValue(), equalTo(27L));
        }
        {
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                .put(INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
                .build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> b.field("@timestamp", "2012-02-13")));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
    }

    public void testRecoverySourceWithColumnarLogsdb() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> { b.field("@timestamp", "2012-02-13"); }));
            assertNotNull(doc.rootDoc().getField("_recovery_source_size"));
            assertThat(doc.rootDoc().getField("_recovery_source_size").numericValue(), equalTo(27L));
        }
        {
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName())
                .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
                .build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> { b.field("@timestamp", "2012-02-13"); }));
            assertNotNull(doc.rootDoc().getField("_recovery_source_size"));
            assertThat(doc.rootDoc().getField("_recovery_source_size").numericValue(), equalTo(27L));
        }
        {
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName())
                .put(INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
                .build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {}));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> b.field("@timestamp", "2012-02-13")));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
    }

    public void testStandardIndexModeWithSourceModeSetting() throws IOException {
        // Test for IndexMode.STANDARD
        {
            final XContentBuilder mappings = topMapping(b -> {});
            final Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.name())
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC)
                .build();
            final MapperService mapperService = createMapperService(settings, mappings);
            DocumentMapper docMapper = mapperService.documentMapper();
            assertTrue(docMapper.sourceMapper().isSynthetic());
        }
        {
            final XContentBuilder mappings = topMapping(b -> {});
            final Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.name())
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED)
                .build();
            final MapperService mapperService = createMapperService(settings, mappings);
            final DocumentMapper docMapper = mapperService.documentMapper();
            assertTrue(docMapper.sourceMapper().isStored());
        }
        {
            final XContentBuilder mappings = topMapping(b -> {});
            final Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.name())
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.DISABLED)
                .build();
            final MapperService mapperService = createMapperService(settings, mappings);
            final DocumentMapper docMapper = mapperService.documentMapper();
            assertTrue(docMapper.sourceMapper().isDisabled());
        }

        // Test for IndexMode.LOGSDB
        {
            final XContentBuilder mappings = topMapping(b -> {});
            final Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC)
                .build();
            final MapperService mapperService = createMapperService(settings, mappings);
            DocumentMapper docMapper = mapperService.documentMapper();
            assertTrue(docMapper.sourceMapper().isSynthetic());
        }
        {
            final XContentBuilder mappings = topMapping(b -> {});
            final Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED)
                .build();
            final MapperService mapperService = createMapperService(settings, mappings);
            final DocumentMapper docMapper = mapperService.documentMapper();
            assertTrue(docMapper.sourceMapper().isStored());
        }
        {
            final XContentBuilder mappings = topMapping(b -> {});
            final Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.DISABLED)
                .build();
            var ex = expectThrows(MapperParsingException.class, () -> createMapperService(settings, mappings));
            assertEquals("Failed to parse mapping: _source can not be disabled in index using [logsdb] index mode", ex.getMessage());
        }

        if (IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled()) {
            // Test for IndexMode.COLUMNAR
            {
                final XContentBuilder mappings = topMapping(b -> {});
                final Settings settings = Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.name())
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC)
                    .build();
                final MapperService mapperService = createMapperService(settings, mappings);
                DocumentMapper docMapper = mapperService.documentMapper();
                assertTrue(docMapper.sourceMapper().isSynthetic());
            }
            {
                final XContentBuilder mappings = topMapping(b -> {});
                final Settings settings = Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.name())
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED)
                    .build();
                expectThrows(IllegalArgumentException.class, () -> createMapperService(settings, mappings));
            }
            {
                final XContentBuilder mappings = topMapping(b -> {});
                final Settings settings = Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.name())
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.DISABLED)
                    .build();
                expectThrows(IllegalArgumentException.class, () -> createMapperService(settings, mappings));
            }

            // Test for IndexMode.LOGSDB_COLUMNAR
            {
                final XContentBuilder mappings = topMapping(b -> {});
                final Settings settings = Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.name())
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC)
                    .build();
                final MapperService mapperService = createMapperService(settings, mappings);
                DocumentMapper docMapper = mapperService.documentMapper();
                assertTrue(docMapper.sourceMapper().isSynthetic());
            }
            {
                final XContentBuilder mappings = topMapping(b -> {});
                final Settings settings = Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.name())
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED)
                    .build();
                expectThrows(IllegalArgumentException.class, () -> createMapperService(settings, mappings));
            }
            {
                final XContentBuilder mappings = topMapping(b -> {});
                final Settings settings = Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.name())
                    .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.DISABLED)
                    .build();
                expectThrows(IllegalArgumentException.class, () -> createMapperService(settings, mappings));
            }
        }

        // Test for IndexMode.TIME_SERIES
        {
            final String mappings = """
                    {
                        "_doc" : {
                            "properties": {
                                "routing_field": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        }
                    }
                """;
            final Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC)
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "routing_field")
                .build();
            final MapperService mapperService = createMapperService(settings, mappings);
            DocumentMapper docMapper = mapperService.documentMapper();
            assertTrue(docMapper.sourceMapper().isSynthetic());
        }
        {
            final String mappings = """
                    {
                        "_doc" : {
                            "properties": {
                                "routing_field": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        }
                    }
                """;
            final Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED)
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "routing_field")
                .build();
            final MapperService mapperService = createMapperService(settings, mappings);
            final DocumentMapper docMapper = mapperService.documentMapper();
            assertTrue(docMapper.sourceMapper().isStored());
        }
        {
            final String mappings = """
                    {
                        "_doc" : {
                            "properties": {
                                "routing_field": {
                                    "type": "keyword",
                                    "time_series_dimension": true
                                }
                            }
                        }
                    }
                """;
            final Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.DISABLED)
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "routing_field")
                .build();
            var ex = expectThrows(MapperParsingException.class, () -> createMapperService(settings, mappings));
            assertEquals("Failed to parse mapping: _source can not be disabled in index using [time_series] index mode", ex.getMessage());
        }

        // Test cases without IndexMode (default to standard)
        {
            final XContentBuilder mappings = topMapping(b -> {});
            final Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC)
                .build();
            final MapperService mapperService = createMapperService(settings, mappings);
            DocumentMapper docMapper = mapperService.documentMapper();
            assertTrue(docMapper.sourceMapper().isSynthetic());
        }
        {
            final XContentBuilder mappings = topMapping(b -> {});
            final Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED)
                .build();
            final MapperService mapperService = createMapperService(settings, mappings);
            final DocumentMapper docMapper = mapperService.documentMapper();
            assertTrue(docMapper.sourceMapper().isStored());
        }
        {
            final XContentBuilder mappings = topMapping(b -> {});
            final Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.DISABLED)
                .build();
            final MapperService mapperService = createMapperService(settings, mappings);
            final DocumentMapper docMapper = mapperService.documentMapper();
            assertTrue(docMapper.sourceMapper().isDisabled());
        }
    }

    public void testRecoverySourceWithLogsCustom() throws IOException {
        XContentBuilder mappings = topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("mode", "synthetic").endObject());
        {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()).build();
            MapperService mapperService = createMapperService(settings, mappings);
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> { b.field("@timestamp", "2012-02-13"); }));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
        {
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName())
                .put(INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
                .build();
            MapperService mapperService = createMapperService(settings, mappings);
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source(b -> b.field("@timestamp", "2012-02-13")));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
    }

    public void testRecoverySourceWithTimeSeries() throws IOException {
        {
            Settings.Builder settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field")
                // Synthetic _ids are not relevant to this test and the "123" id used is not synthetic-id-conformant
                .put(IndexSettings.SYNTHETIC_ID.getKey(), false);
            MapperService mapperService = createMapperService(settings.build(), fieldMapping(b -> {
                b.field("type", "keyword");
                b.field("time_series_dimension", true);
            }));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source("123", b -> b.field("@timestamp", "2012-02-13").field("field", "value1"), null));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
        {
            Settings.Builder settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field")
                .put(INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
                // Synthetic _ids are not relevant to this test and the "123" id used is not synthetic-id-conformant
                .put(IndexSettings.SYNTHETIC_ID.getKey(), false);
            MapperService mapperService = createMapperService(settings.build(), fieldMapping(b -> {
                b.field("type", "keyword");
                b.field("time_series_dimension", true);
            }));
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(
                source("123", b -> b.field("@timestamp", "2012-02-13").field("field", randomAlphaOfLength(5)), null)
            );
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
    }

    public void testRecoverySourceWithTimeSeriesCustom() throws IOException {
        String mappings = """
                {
                    "_doc" : {
                        "_source" : {
                            "mode" : "synthetic"
                        },
                        "properties": {
                            "field": {
                                "type": "keyword",
                                "time_series_dimension": true
                            }
                        }
                    }
                }
            """;
        {
            Settings.Builder settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field")
                // Synthetic _ids are not relevant to this test and the "123" id used is not synthetic-id-conformant
                .put(IndexSettings.SYNTHETIC_ID.getKey(), false);
            MapperService mapperService = createMapperService(settings.build(), mappings);
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(source("123", b -> b.field("@timestamp", "2012-02-13").field("field", "value1"), null));
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
        {
            Settings.Builder settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field")
                .put(INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
                // Synthetic _ids are not relevant to this test and the "123" id used is not synthetic-id-conformant
                .put(IndexSettings.SYNTHETIC_ID.getKey(), false);
            MapperService mapperService = createMapperService(settings.build(), mappings);
            DocumentMapper docMapper = mapperService.documentMapper();
            ParsedDocument doc = docMapper.parse(
                source("123", b -> b.field("@timestamp", "2012-02-13").field("field", randomAlphaOfLength(5)), null)
            );
            assertNull(doc.rootDoc().getField("_recovery_source"));
        }
    }
}
