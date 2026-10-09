/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matcher;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MappedIndexWriterTests extends MapperServiceTestCase {

    public void testBuilderWritesColumnarFormat() throws IOException {
        assumeColumnarCodec();
        assertWritesMappedFormat(columnar(true), this::mappedWriter, instanceOf(ColumNARDocValuesFormat.class));
    }

    public void testBuilderWritesEs819Format() throws IOException {
        assertWritesMappedFormat(columnar(false), this::mappedWriter, instanceOf(ES819Version3TSDBDocValuesFormat.class));
    }

    public void testBuilderWritesEs95Format() throws IOException {
        assertWritesMappedFormat(timeSeriesEs95(), this::mappedWriter, instanceOf(ES95TSDBDocValuesFormat.class));
    }

    public void testSyntheticSourceWriterWritesColumnarFormat() throws IOException {
        assumeColumnarCodec();
        assertWritesMappedFormat(columnar(true), this::indexWriterForSyntheticSource, instanceOf(ColumNARDocValuesFormat.class));
    }

    public void testSyntheticSourceWriterWritesEs819Format() throws IOException {
        assertWritesMappedFormat(columnar(false), this::indexWriterForSyntheticSource, instanceOf(ES819Version3TSDBDocValuesFormat.class));
    }

    public void testSyntheticSourceWriterWritesEs95Format() throws IOException {
        assertWritesMappedFormat(timeSeriesEs95(), this::indexWriterForSyntheticSource, instanceOf(ES95TSDBDocValuesFormat.class));
    }

    public void testAssertDocValuesWrittenAsMappedRejectsUnmappedWriter() throws IOException {
        final MapperService mapperService = createMapperService(columnar(false), keywordMapping(false));
        final String expected = productionDocValuesFormat(mapperService, "foo").getName();
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                iw.addDocuments(mapperService.documentMapper().parse(source(b -> b.field("foo", "value"))).docs());
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final AssertionError e = expectThrows(AssertionError.class, () -> assertDocValuesWrittenAsMapped(mapperService, reader));
                assertThat(e.getMessage(), containsString("foo=mapped to [" + expected + "]"));
            }
        }
    }

    public void testBuilderRequiresMapperService() {
        expectThrows(NullPointerException.class, () -> TestIndexWriterBuilder.mapped(null));
    }

    private RandomIndexWriter mappedWriter(MapperService mapperService, Directory directory) throws IOException {
        return TestIndexWriterBuilder.mapped(mapperService).build(directory);
    }

    private static void assumeColumnarCodec() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
    }

    private static Settings columnar(boolean columnarCodec) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), columnarCodec)
            .build();
    }

    private static Settings timeSeriesEs95() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-10-29T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_ES95_CODEC_ENABLED_SETTING.getKey(), true)
            // NOTE: synthetic _id postings report a docFreq of 0, which CheckIndex rejects when the test directory closes.
            .put(IndexSettings.SYNTHETIC_ID.getKey(), false)
            .build();
    }

    private XContentBuilder keywordMapping(boolean timeSeries) throws IOException {
        return mapping(b -> {
            b.startObject("foo").field("type", "keyword");
            if (timeSeries) {
                b.field("time_series_dimension", true);
            }
            b.endObject();
        });
    }

    private void assertWritesMappedFormat(
        Settings settings,
        CheckedBiFunction<MapperService, Directory, RandomIndexWriter, IOException> writer,
        Matcher<? super DocValuesFormat> productionFormat
    ) throws IOException {
        final boolean timeSeries = IndexSettings.MODE.get(settings) == IndexMode.TIME_SERIES;
        final MapperService mapperService = createMapperService(settings, keywordMapping(timeSeries));
        final DocValuesFormat expected = productionDocValuesFormat(mapperService, "foo");
        assertThat("production picks a specialized format for [foo]", expected, productionFormat);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = writer.apply(mapperService, directory)) {
                iw.addDocuments(mapperService.documentMapper().parse(source(timeSeries ? null : "1", b -> {
                    b.field("foo", "value");
                    if (timeSeries) {
                        b.field("@timestamp", "2021-10-01");
                    }
                }, timeSeries ? TimeSeriesRoutingHashFieldMapper.encode(0) : null)).docs());
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    final FieldInfo foo = leaf.reader().getFieldInfos().fieldInfo("foo");
                    assertThat(foo.getAttribute(PerFieldDocValuesFormat.PER_FIELD_FORMAT_KEY), equalTo(expected.getName()));
                }
                assertDocValuesWrittenAsMapped(mapperService, reader);
            }
        }
    }
}
