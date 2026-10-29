/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Reads numeric doc values the ColumNAR codec stores back through {@link ColumnarNumericDocValues}, as every reader of numeric
 * doc values does, and checks they come back as {@code SORTED_NUMERIC} would hold them.
 */
public class ColumnarNumericDocValuesTests extends MapperServiceTestCase {

    @Before
    public void assumeNumericCodecFlagEnabled() {
        assumeTrue(
            "columnar_codec_numeric feature flag must be enabled",
            ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_NUMERIC_FEATURE_FLAG.isEnabled()
        );
    }

    public void testSingleValuedColumnIsServedAsSingleton() throws IOException {
        final Map<Long, long[]> expected = new HashMap<>();
        final int docs = between(1, 200);
        for (long id = 0; id < docs; id++) {
            expected.put(id, randomBoolean() ? new long[0] : new long[] { randomLong() });
        }
        assertReadsBack(columnarMapperService(true), expected, true);
    }

    public void testMultiValuedColumnIsSortedWithDuplicates() throws IOException {
        final Map<Long, long[]> expected = new HashMap<>();
        final int docs = between(1, 200);
        for (long id = 0; id < docs; id++) {
            final long[] values = new long[between(0, 5)];
            for (int i = 0; i < values.length; i++) {
                values[i] = randomBoolean() ? randomLongBetween(-3, 3) : randomLong();
            }
            expected.put(id, values);
        }
        // At least one document holds two values, so the column is multi-valued.
        expected.put((long) docs, new long[] { 5, 5 });
        assertReadsBack(columnarMapperService(true), expected, false);
    }

    public void testSortedNumericDocValuesPassThrough() throws IOException {
        final Map<Long, long[]> expected = new HashMap<>();
        final int docs = between(1, 50);
        for (long id = 0; id < docs; id++) {
            expected.put(id, new long[] { randomLong(), randomLong() });
        }
        assertReadsBack(columnarMapperService(false), expected, false);
    }

    private MapperService columnarMapperService(boolean codec) throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.COLUMNAR_CODEC_NUMERIC_ENABLED_SETTING.getKey(), codec)
            .build();
        return createMapperService(settings, mapping(b -> {
            b.startObject("id").field("type", "long").field("multi_value", false).endObject();
            b.startObject("field").field("type", "long").endObject();
        }));
    }

    private void assertReadsBack(MapperService mapperService, Map<Long, long[]> expected, boolean singleValued) throws IOException {
        final DocValuesType expectedType = mapperService.getIndexSettings().isColumnarCodecNumericEnabled()
            ? DocValuesType.BINARY
            : DocValuesType.SORTED_NUMERIC;
        withLuceneIndex(mapperService, iw -> {
            for (Map.Entry<Long, long[]> doc : expected.entrySet()) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> {
                    b.field("id", doc.getKey());
                    long[] values = doc.getValue();
                    if (values.length == 1 && randomBoolean()) {
                        b.field("field", values[0]);
                    } else if (values.length > 0) {
                        b.array("field", values);
                    }
                })).rootDoc());
            }
        }, reader -> {
            final Map<Long, long[]> actual = new HashMap<>();
            for (LeafReaderContext context : reader.leaves()) {
                final LeafReader leaf = context.reader();
                assertThat(leaf.getFieldInfos().fieldInfo("id").getDocValuesType(), equalTo(expectedType));
                final NumericDocValues ids = DocValues.unwrapSingleton(ColumnarNumericDocValues.getSortedNumeric(leaf, "id"));
                assertThat("multi_value=false ids are single-valued", ids, notNullValue());
                final SortedNumericDocValues values = ColumnarNumericDocValues.getSortedNumeric(leaf, "field");
                if (singleValued) {
                    assertThat("a single-valued column unwraps", DocValues.unwrapSingleton(values), notNullValue());
                } else if (expectedType == DocValuesType.BINARY && leafHasMultipleValues(leaf, expected)) {
                    assertThat("a multi-valued column doesn't unwrap", DocValues.unwrapSingleton(values), nullValue());
                }
                for (int doc = 0; doc < leaf.maxDoc(); doc++) {
                    assertTrue(ids.advanceExact(doc));
                    final long id = ids.longValue();
                    if (values.advanceExact(doc)) {
                        final long[] read = new long[values.docValueCount()];
                        for (int i = 0; i < read.length; i++) {
                            read[i] = values.nextValue();
                        }
                        actual.put(id, read);
                    } else {
                        actual.put(id, new long[0]);
                    }
                }
            }
            assertThat(actual.keySet(), equalTo(expected.keySet()));
            for (Map.Entry<Long, long[]> doc : expected.entrySet()) {
                final long[] sorted = doc.getValue().clone();
                Arrays.sort(sorted);
                assertArrayEquals("doc " + doc.getKey(), sorted, actual.get(doc.getKey()));
            }
        });
    }

    /** Whether any document of {@code leaf} holds more than one value, which decides whether its column can be single-valued. */
    private static boolean leafHasMultipleValues(LeafReader leaf, Map<Long, long[]> expected) throws IOException {
        final NumericDocValues ids = DocValues.unwrapSingleton(ColumnarNumericDocValues.getSortedNumeric(leaf, "id"));
        final List<Long> idsInLeaf = new ArrayList<>();
        while (ids.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
            idsInLeaf.add(ids.longValue());
        }
        return idsInLeaf.stream().anyMatch(id -> expected.get(id).length > 1);
    }
}
