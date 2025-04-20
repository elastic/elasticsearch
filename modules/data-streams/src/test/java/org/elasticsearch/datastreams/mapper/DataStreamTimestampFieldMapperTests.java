/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.mapper;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataMapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DataStreamTimestampFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return DataStreamTimestampFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return true;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "enabled",
            timestampMapping(true, b -> b.startObject("@timestamp").field("type", "date").endObject()),
            timestampMapping(false, b -> b.startObject("@timestamp").field("type", "date").endObject()),
            dm -> {}
        );
        checker.registerUpdateCheck(
            timestampMapping(false, b -> b.startObject("@timestamp").field("type", "date").endObject()),
            timestampMapping(true, b -> b.startObject("@timestamp").field("type", "date").endObject()),
            dm -> assertTrue(dm.metadataMapper(DataStreamTimestampFieldMapper.class).isEnabled())
        );
    }

    private static XContentBuilder timestampMapping(boolean enabled, CheckedConsumer<XContentBuilder, IOException> propertiesBuilder)
        throws IOException {
        return topMapping(b -> {
            b.startObject(DataStreamTimestampFieldMapper.NAME).field("enabled", enabled).endObject();
            b.startObject("properties");
            propertiesBuilder.accept(b);
            b.endObject();
        });
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new DataStreamsPlugin(Settings.EMPTY));
    }

    public void testPostParse() throws IOException {
        DocumentMapper docMapper = createDocumentMapper(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", randomBoolean() ? "date" : "date_nanos");
            b.endObject();
        }));

        ParsedDocument doc = docMapper.parse(source(b -> b.field("@timestamp", "2020-12-12")));
        assertThat(doc.rootDoc().getFields("@timestamp").size(), equalTo(1));

        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> docMapper.parse(source(b -> b.field("@timestamp1", "2020-12-12")))
        );
        assertThat(e.getCause().getMessage(), equalTo("data stream timestamp field [@timestamp] is missing"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> docMapper.parse(source(b -> b.array("@timestamp", "2020-12-12", "2020-12-13")))
        );
        assertThat(e.getCause().getMessage(), equalTo("data stream timestamp field [@timestamp] encountered multiple values"));
    }

    public void testValidateNonExistingField() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(timestampMapping(true, b -> b.startObject("my_date_field").field("type", "date").endObject()))
        );
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] does not exist"));
    }

    public void testValidateInvalidFieldType() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createMapperService(timestampMapping(true, b -> b.startObject("@timestamp").field("type", "keyword").endObject()))
        );
        assertThat(
            e.getMessage(),
            equalTo("data stream timestamp field [@timestamp] is of type [keyword], but [date,date_nanos] is expected")
        );
    }

    public void testValidateNotIndexed() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("index", false);
            b.endObject();
        })));
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] is not indexed"));
    }

    public void testValidateNotDocValues() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("doc_values", false);
            b.endObject();
        })));
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] doesn't have doc values"));
    }

    public void testValidateNullValue() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("null_value", "2020-12-12");
            b.endObject();
        })));
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] has disallowed [null_value] attribute specified"));
    }

    public void testValidateIgnoreMalformed() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("ignore_malformed", true);
            b.endObject();
        })));
        assertThat(
            e.getMessage(),
            equalTo("data stream timestamp field [@timestamp] has disallowed [ignore_malformed] attribute specified")
        );
    }

    public void testValidateNotDisallowedAttribute() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperService(timestampMapping(true, b -> {
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("store", true);
            b.endObject();
        })));
        assertThat(e.getMessage(), equalTo("data stream timestamp field [@timestamp] has disallowed attributes: [store]"));
    }

    public void testValidateDefaultIgnoreMalformed() throws Exception {
        Settings indexSettings = Settings.builder().put(FieldMapper.IGNORE_MALFORMED_SETTING.getKey(), true).build();
        {
            MapperService mapperService = createMapperService(
                IndexVersion.current(),
                indexSettings,
                () -> true,
                timestampMapping(true, b -> {
                    b.startObject("@timestamp");
                    b.field("type", "date");
                    b.endObject();
                    b.startObject("summary");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("@timestamp");
                            b.field("type", "date");
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                })
            );
            assertThat(mapperService, notNullValue());
            assertThat(mapperService.documentMapper().mappers().getMapper("@timestamp"), notNullValue());
            assertThat(((DateFieldMapper) mapperService.documentMapper().mappers().getMapper("@timestamp")).ignoreMalformed(), is(false));
            DateFieldMapper summaryTimestamp = (DateFieldMapper) (mapperService.documentMapper()
                .mappers()
                .objectMappers()
                .get("summary")
                .getMapper("@timestamp"));
            assertThat(summaryTimestamp, notNullValue());
            assertThat(summaryTimestamp.ignoreMalformed(), is(true));
        }
        {
            MapperService mapperService = createMapperService(
                IndexVersion.current(),
                indexSettings,
                () -> true,
                timestampMapping(true, b -> {
                    b.startObject("@timestamp");
                    b.field("type", "date");
                    b.field("ignore_malformed", false);
                    b.endObject();
                    b.startObject("summary.@timestamp");
                    b.field("type", "date");
                    b.field("ignore_malformed", false);
                    b.endObject();
                })
            );
            assertThat(mapperService, notNullValue());
            assertThat(mapperService.documentMapper().mappers().getMapper("@timestamp"), notNullValue());
            assertThat(((DateFieldMapper) mapperService.documentMapper().mappers().getMapper("@timestamp")).ignoreMalformed(), is(false));
            DateFieldMapper summaryTimestamp = (DateFieldMapper) (mapperService.documentMapper()
                .mappers()
                .objectMappers()
                .get("summary")
                .getMapper("@timestamp"));
            assertThat(summaryTimestamp, notNullValue());
            assertThat(summaryTimestamp.ignoreMalformed(), is(false));
        }
    }

    public void testFieldTypeWithDocValuesSkipper_LogsDBModeDisabledDocValuesSkipper() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), false)
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            b.field("type", "date");
            b.endObject();
        }));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        assertTrue(timestampMapper.fieldType().isIndexed());
    }

    public void testFieldTypeWithDocValuesSkipper_LogsDBMode() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            b.field("type", "date");
            b.endObject();
        }));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_LogsDBModeNoTimestampMapping() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {}));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_LogsDBModeTimestampDateNanos() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .build();
        final MapperService mapperService = withMapping(
            new TestMapperServiceBuilder().settings(settings).applyDefaultMapping(false).build(),
            timestampMapping(true, b -> {
                b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                b.field("type", "date_nanos");
                b.endObject();
            })
        );

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_LogsDBModeExplicitTimestampIndexEnabledDocValuesSkipper() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            b.field("type", "date");
            b.field("index", true);
            b.endObject();
        }));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_LogsDBModeExplicitTimestampIndexDisabledDocValuesSkipper() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), false)
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            b.field("type", "date");
            b.field("index", true);
            b.endObject();
        }));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        assertTrue(timestampMapper.fieldType().isIndexed());
    }

    public void testFieldTypeWithDocValuesSkipper_LogsDBModeWithoutDefaultMapping() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .build();
        final MapperService mapperService = withMapping(
            new TestMapperServiceBuilder().settings(settings).applyDefaultMapping(false).build(),
            timestampMapping(true, b -> {
                b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                b.field("type", "date");
                b.endObject();
            })
        );

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_DocValuesFalseEnabledDocValuesSkipper() {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .build();
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> withMapping(
                new TestMapperServiceBuilder().settings(settings).applyDefaultMapping(false).build(),
                timestampMapping(true, b -> {
                    b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    b.field("type", "date");
                    b.field("doc_values", false);
                    b.endObject();
                })
            )
        );
        assertEquals(
            ex.getMessage(),
            "data stream timestamp field [" + DataStreamTimestampFieldMapper.DEFAULT_PATH + "] doesn't have doc values"
        );
    }

    public void testFieldTypeWithDocValuesSkipper_DocValuesFalseDisabledDocValuesSkipper() {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), false)
            .build();
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> withMapping(
                new TestMapperServiceBuilder().settings(settings).applyDefaultMapping(false).build(),
                timestampMapping(true, b -> {
                    b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                    b.field("type", "date");
                    b.field("doc_values", false);
                    b.endObject();
                })
            )
        );
        assertEquals(
            ex.getMessage(),
            "data stream timestamp field [" + DataStreamTimestampFieldMapper.DEFAULT_PATH + "] doesn't have doc values"
        );
    }

    public void testFieldTypeWithDocValuesSkipper_WithoutTimestampSorting() throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name()).build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            b.field("type", "date");
            b.endObject();
        }));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        // NOTE: in LogsDB we always sort on @timestamp (and maybe also on host.name) by default
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_StandardMode() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            b.field("type", "date");
            b.endObject();
        }));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        assertTrue(timestampMapper.fieldType().isIndexed());
        assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
    }

    public void testFieldTypeWithDocValuesSkipper_CustomTimestampField() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.name())
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject("timestamp");
            b.field("type", "date");
            b.endObject();
        }));

        final DateFieldMapper customTimestamp = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("timestamp");
        assertTrue(customTimestamp.fieldType().hasDocValues());
        assertTrue(customTimestamp.fieldType().isIndexed());
        assertFalse(customTimestamp.fieldType().hasDocValuesSkipper());

        // Default LogsDB mapping including @timestamp field is used
        final DateFieldMapper defaultTimestamp = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("@timestamp");
        assertTrue(defaultTimestamp.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(defaultTimestamp.fieldType().isIndexed());
            assertTrue(defaultTimestamp.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(defaultTimestamp.fieldType().isIndexed());
            assertFalse(defaultTimestamp.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_TSDBModeDisabledDocValuesSkipper() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), false)
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            b.field("type", "date");
            b.endObject();
        }));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        assertTrue(timestampMapper.fieldType().isIndexed());
    }

    public void testFieldTypeWithDocValuesSkipper_TSDBMode() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            b.field("type", "date");
            b.endObject();
        }));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_TSDBModeNoTimestampMapping() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {}));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_TSDBModeTimestampDateNanos() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();
        final MapperService mapperService = withMapping(
            new TestMapperServiceBuilder().settings(settings).applyDefaultMapping(false).build(),
            timestampMapping(true, b -> {
                b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                b.field("type", "date_nanos");
                b.endObject();
            })
        );

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_TSDBModeExplicitTimestampIndexEnabledDocValuesSkipper() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            b.field("type", "date");
            b.field("index", true);
            b.endObject();
        }));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }

    public void testFieldTypeWithDocValuesSkipper_TSDBModeExplicitTimestampIndexDisabledDocValuesSkipper() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), false)
            .build();
        final MapperService mapperService = createMapperService(settings, timestampMapping(true, b -> {
            b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            b.field("type", "date");
            b.field("index", true);
            b.endObject();
        }));

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        assertTrue(timestampMapper.fieldType().isIndexed());
    }

    public void testFieldTypeWithDocValuesSkipper_TSDBModeWithoutDefaultMapping() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .build();
        final MapperService mapperService = withMapping(
            new TestMapperServiceBuilder().settings(settings).applyDefaultMapping(false).build(),
            timestampMapping(true, b -> {
                b.startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH);
                b.field("type", "date");
                b.endObject();
            })
        );

        final DateFieldMapper timestampMapper = (DateFieldMapper) mapperService.documentMapper()
            .mappers()
            .getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        assertTrue(timestampMapper.fieldType().hasDocValues());
        if (IndexSettings.USE_DOC_VALUES_SKIPPER.get(settings)) {
            assumeTrue("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER);
            assertFalse(timestampMapper.fieldType().isIndexed());
            assertTrue(timestampMapper.fieldType().hasDocValuesSkipper());
        } else {
            // TODO: remove this 'else' branch when removing the `doc_values_skipper` feature flag
            assumeFalse("doc_values_skipper feature flag enabled", IndexSettings.DOC_VALUES_SKIPPER == false);
            assertTrue(timestampMapper.fieldType().isIndexed());
            assertFalse(timestampMapper.fieldType().hasDocValuesSkipper());
        }
    }
}
