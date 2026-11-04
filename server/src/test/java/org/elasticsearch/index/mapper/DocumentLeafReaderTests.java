/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.fielddata.ColumnarNumericDocValues;

import java.io.IOException;
import java.util.Map;

/**
 * {@link DocumentLeafReader} serves the document being parsed to index-time scripts, without a codec in between.
 */
public class DocumentLeafReaderTests extends MapperServiceTestCase {

    /**
     * A numeric field whose doc values the ColumNAR codec stores writes them as binary, but its readers ask for
     * {@code SORTED_NUMERIC}, so the reader serves its values that way, sorted with duplicates kept, just as it serves a field
     * written as {@code SORTED_NUMERIC}.
     */
    public void testColumnarNumericIsServedAsSortedNumeric() throws IOException {
        assumeTrue(
            "columnar_codec_numeric feature flag must be enabled",
            ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_NUMERIC_FEATURE_FLAG.isEnabled()
        );
        final boolean codec = randomBoolean();
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.COLUMNAR_CODEC_NUMERIC_ENABLED_SETTING.getKey(), codec)
            .build();
        final MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("n").field("type", "long").endObject();
            b.startObject("absent").field("type", "long").endObject();
        }));
        final LuceneDocument doc = mapperService.documentMapper().parse(source(b -> b.array("n", 7, -2, 7, 3))).rootDoc();
        assertEquals(
            codec ? DocValuesType.BINARY : DocValuesType.SORTED_NUMERIC,
            doc.getFields("n").stream().map(f -> f.fieldType().docValuesType()).filter(t -> t != DocValuesType.NONE).findFirst().get()
        );

        final DocumentLeafReader reader = new DocumentLeafReader(doc, Map.of());
        final SortedNumericDocValues values = ColumnarNumericDocValues.getSortedNumeric(reader, "n");
        assertTrue(values.advanceExact(0));
        assertEquals(4, values.docValueCount());
        for (long expected : new long[] { -2, 3, 7, 7 }) {
            assertEquals(expected, values.nextValue());
        }
        assertNull(reader.getSortedNumericDocValues("absent"));
    }
}
