/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * What a keyword stored as a ColumNAR payload hands back at the fielddata surface, read out of a real index rather
 * than a hand-built column: the order it was written in, duplicates and all. The sibling
 * {@link AbstractColumnarArrayOrderFieldDataTestCase} covers the layout a strictly columnar index uses without the
 * codec, which still sorts, so this is the only place the two part company.
 */
@ESTestCase.WithoutEntitlements // the codec feature flag is read from a system property
public class KeywordColumnarPayloadFieldDataTests extends MapperServiceTestCase {

    @Override
    protected Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
            .build();
    }

    public void testValuesComeBackInTheOrderTheyWereWritten() throws IOException {
        assumeTrue("the ColumNAR codec is behind a feature flag", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
        assertThat(read("c", "a", "b"), contains("c", "a", "b"));
    }

    /** Nothing deduplicates on the way out, and a repeat need not sit next to the value it repeats. */
    public void testRepeatsAreKeptWhereverTheyFall() throws IOException {
        assumeTrue("the ColumNAR codec is behind a feature flag", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
        assertThat(read("b", "a", "b"), contains("b", "a", "b"));
    }

    public void testTheFieldSaysItsValuesAreInArrayOrder() throws IOException {
        assumeTrue("the ColumNAR codec is behind a feature flag", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
        final MapperService mapperService = columnarKeyword();
        withValues(mapperService, new String[] { "b", "a" }, values -> {
            assertThat(values.getValueOrder(), equalTo(SortableBinaryDocValues.ValueOrder.ARRAY));
            return null;
        });
    }

    private List<String> read(String... written) throws IOException {
        return withValues(columnarKeyword(), written, values -> {
            final List<String> read = new ArrayList<>();
            assertTrue(values.advanceExact(0));
            for (int i = 0; i < values.docValueCount(); i++) {
                read.add(values.nextValue().utf8ToString());
            }
            return read;
        });
    }

    private MapperService columnarKeyword() throws IOException {
        return createMapperService(getIndexSettings(), mapping(b -> b.startObject("field").field("type", "keyword").endObject()));
    }

    private interface Read<T> {
        T read(SortableBinaryDocValues values) throws IOException;
    }

    private <T> T withValues(MapperService mapperService, String[] written, Read<T> read) throws IOException {
        final List<T> out = new ArrayList<>();
        withLuceneIndex(
            mapperService,
            iw -> iw.addDocument(mapperService.documentMapper().parse(source(b -> b.array("field", written))).rootDoc()),
            reader -> {
                final LeafReaderContext leaf = reader.leaves().get(0);
                final IndexFieldData<?> fieldData = mapperService.fieldType("field")
                    .fielddataBuilder(FieldDataContext.noRuntimeFields("test", "test"))
                    .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
                out.add(read.read(fieldData.load(leaf).getBytesValues()));
            }
        );
        return out.get(0);
    }
}
