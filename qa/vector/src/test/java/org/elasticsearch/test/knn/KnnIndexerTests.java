/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/** Verifies the Lucene document layouts emitted by the vector QA indexer. */
public class KnnIndexerTests extends ESTestCase {

    public void testSlicedPartitionDocumentUsesProductionSliceLayout() {
        final String partition = "partition_000001";
        KnnIndexer.PartitionDocumentFactory factory = new KnnIndexer.PartitionDocumentFactory(
            new String[] { partition },
            new int[] { 7 },
            true
        );

        Document document = factory.createDocument(new StringField("placeholder", "value", Field.Store.NO), 0);

        assertThat(document.getField(KnnIndexer.PARTITION_ID_FIELD).binaryValue(), equalTo(new BytesRef(partition)));
        assertThat(document.getField(SliceIndexing.SLICE_KEY_FIELD_NAME).binaryValue(), equalTo(SliceIndexing.encodeSliceKey(partition)));
        assertThat(document.getField(SliceIndexing.SLICE_KEY_FIELD_NAME).fieldType().docValuesType(), equalTo(DocValuesType.SORTED));
        assertThat(
            document.getField(SliceIndexing.SLICE_HASH_FIELD_NAME).numericValue().longValue(),
            equalTo(SliceIndexing.sliceHash(partition))
        );
        assertThat(
            document.getField(SliceIndexing.SLICE_HASH_FIELD_NAME).fieldType().docValuesType(),
            equalTo(DocValuesType.SORTED_NUMERIC)
        );
        assertThat(
            document.getField(SliceIndexing.SLICE_HASH_FIELD_NAME).fieldType().docValuesSkipIndexType(),
            not(equalTo(DocValuesSkipIndexType.NONE))
        );
    }

    public void testUnslicedPartitionDocumentDoesNotAddSliceFields() {
        KnnIndexer.PartitionDocumentFactory factory = new KnnIndexer.PartitionDocumentFactory(
            new String[] { "partition_000001" },
            new int[] { 7 },
            false
        );

        Document document = factory.createDocument(new StringField("placeholder", "value", Field.Store.NO), 0);

        assertNull(document.getField(SliceIndexing.SLICE_KEY_FIELD_NAME));
        assertNull(document.getField(SliceIndexing.SLICE_HASH_FIELD_NAME));
    }
}
