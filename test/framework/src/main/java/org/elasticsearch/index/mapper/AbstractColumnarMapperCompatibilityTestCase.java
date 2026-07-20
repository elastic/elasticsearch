/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Abstract base for compatibility tests that verify the columnar batch-mapping path produces the
 * same Lucene fields as the conventional x-content parse path for the same documents.
 * <p>
 * Subclasses write {@code public void testXxx()} methods and call
 * {@link #assertColumnarMatchesXContent(XContentBuilder, Settings, Batch...)} directly, passing
 * their own mapping, index settings, and batches. This allows a single subclass to define multiple
 * independent tests with different mappings (e.g. one per routing variant).
 *
 * @see MetadataFieldMapper#preColumnarParse(BatchMappingContext)
 * @see MetadataFieldMapper#postColumnarParse(BatchMappingContext)
 * @see MappedColumns#rowCursor()
 * @see MappedColumns#toColumnBatch()
 */
public abstract class AbstractColumnarMapperCompatibilityTestCase extends MapperServiceTestCase {

    /**
     * A single document input for a compatibility scenario.
     */
    // We can add an x-content builder variant of this if String is too simple for complex scenarios.
    protected record Doc(String id, @Nullable String routing, long seqNo, long version, String source) {}

    /**
     * A named batch of documents that are mapped together in a single columnar pass, then
     * compared individually against x-content parse results.
     *
     * @param primaryTerm the primary term the "engine" will assign to every document in the batch
     */
    protected record Batch(String name, long primaryTerm, List<Doc> docs) {}

    /**
     * Runs the parity check for the given mapping, index settings, and scenarios. Builds a
     * {@link MapperService} from the supplied mapping and settings, then calls
     * {@link #assertScenario} for each scenario.
     */
    protected final void assertColumnarMatchesXContent(XContentBuilder mapping, Settings indexSettings, Batch... scenarios)
        throws IOException {
        final MapperService mapperService = createMapperService(indexSettings, mapping);
        for (Batch scenario : scenarios) {
            assertScenario(mapperService, scenario);
        }
    }

    /** Creates a {@link Doc} with no routing and version {@code 1}. */
    protected static Doc doc(String id, long seqNo, String source) {
        return new Doc(id, null, seqNo, 1L, source);
    }

    /** Creates a {@link Doc} with a routing value and version {@code 1}. */
    protected static Doc doc(String id, @Nullable String routing, long seqNo, String source) {
        return new Doc(id, routing, seqNo, 1L, source);
    }

    /** Creates a {@link Doc} with explicit routing, seqNo, and version. */
    protected static Doc doc(String id, @Nullable String routing, long seqNo, long version, String source) {
        return new Doc(id, routing, seqNo, version, source);
    }

    /** Creates a {@link Batch} with an explicit primary term from a varargs array of {@link Doc}s. */
    protected static Batch batch(String name, long primaryTerm, Doc... docs) {
        return new Batch(name, primaryTerm, List.of(docs));
    }

    /**
     * Maps the scenario's documents through both paths and asserts field-set equality for each
     * document.
     *
     * <p>The harness plays the role of {@code InternalEngine}: it assigns deterministic
     * {@code (seqNo, primaryTerm, version)} values to both paths after parsing, exactly as
     * {@code InternalEngine} does at indexing time.
     */
    private void assertScenario(MapperService mapperService, Batch scenario) throws IOException {
        final List<Doc> docs = scenario.docs();
        final int docCount = docs.size();

        // Build source bytes once so both paths see the same JSON.
        final BytesReference[] sourceBytesArray = new BytesReference[docCount];
        for (int i = 0; i < docCount; i++) {
            sourceBytesArray[i] = new BytesArray(docs.get(i).source().getBytes(StandardCharsets.UTF_8));
        }

        final IndexRequest[] requests = buildIndexRequests(docs, sourceBytesArray);
        final MappingLookup mappingLookup = mapperService.mappingLookup();
        final BatchMappingContext ctx = new BatchMappingContext(requests, mappingLookup, mapperService.getIndexSettings());

        // Drive all supported metadata mappers through their columnar hooks, mirroring the
        // preParse-all / postParse-all ordering of the row-major path.
        final MetadataFieldMapper[] allMetadata = mappingLookup.getMapping().getSortedMetadataMappers();
        final List<MetadataFieldMapper> supportedMappers = Arrays.stream(allMetadata)
            .filter(m -> m.supportsColumnarParse(mapperService.getIndexSettings()))
            .toList();
        for (MetadataFieldMapper m : supportedMappers) {
            m.preColumnarParse(ctx);
        }
        for (MetadataFieldMapper m : supportedMappers) {
            m.postColumnarParse(ctx);
        }

        // Apply engine values to the backing byte arrays before reading via either cursor path.
        final MappedColumns mc = ctx.columns();
        mc.fillPrimaryTerm(scenario.primaryTerm());
        for (int i = 0; i < docCount; i++) {
            mc.setSeqNo(i, docs.get(i).seqNo());
            mc.setVersion(i, docs.get(i).version());
        }

        // Materialize x-content descriptors up front.
        final List<List<FieldDescriptor>> xcDescsPerDoc = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            final Doc doc = docs.get(i);
            final SourceToParse sourceToParse = new SourceToParse(doc.id(), sourceBytesArray[i], XContentType.JSON, doc.routing());
            final ParsedDocument pd = mapperService.documentMapper().parse(sourceToParse);
            // Apply the same engine values as the columnar path (mirrors InternalEngine lines 1910-1911).
            pd.updateSeqID(doc.seqNo(), scenario.primaryTerm());
            pd.version().setLongValue(doc.version());
            xcDescsPerDoc.add(toDescriptors(pd.rootDoc().getFields()));
        }

        // Materialize row-cursor descriptors.
        final List<List<FieldDescriptor>> descsPerDoc = new ArrayList<>(docCount);
        final MappedColumns.RowCursor rowCursor = mc.rowCursor();
        for (int i = 0; i < docCount; i++) {
            rowCursor.advance();
            descsPerDoc.add(toDescriptors(rowCursor.fields()));
        }

        // Compare row-cursor against x-content.
        for (int i = 0; i < docCount; i++) {
            assertFieldSetsEqual(
                xcDescsPerDoc.get(i),
                descsPerDoc.get(i),
                "Batch [" + scenario.name() + "] doc[" + i + "] id=[" + docs.get(i).id() + "]: x-content vs row-cursor field sets differ"
            );
            descsPerDoc.get(i).clear();
        }

        // Populate the same per-doc lists with column-batch descriptors and compare.
        populateColumnBatchDescriptors(mc, descsPerDoc);
        for (int i = 0; i < docCount; i++) {
            assertFieldSetsEqual(
                xcDescsPerDoc.get(i),
                descsPerDoc.get(i),
                "Batch [" + scenario.name() + "] doc[" + i + "] id=[" + docs.get(i).id() + "]: x-content vs column-batch field sets differ"
            );
        }
    }

    private void populateColumnBatchDescriptors(MappedColumns mc, List<List<FieldDescriptor>> perDoc) {
        final ColumnBatch batch = mc.toColumnBatch();
        for (Column column : batch.columns()) {
            final FieldType ft = new FieldType(column.fieldType());
            ft.freeze();
            final String name = column.name();
            // Sparse columns must use tuples() (values() is undefined for absent rows).
            // For dense columns, randomly choose between the tuples and values cursor families
            // so both get exercised. Since tuples() is already verified against x-content, any
            // divergence between the two families will surface as a test failure.
            boolean isSparse = column.density() == Column.Density.SPARSE;
            if (column instanceof LongColumn longColumn) {
                if (isSparse || randomBoolean()) {
                    final LongTupleCursor cursor = longColumn.tuples();
                    for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
                        perDoc.get(doc).add(new FieldDescriptor(name, ft, cursor.longValue(), null));
                    }
                } else {
                    final LongValuesCursor cursor = longColumn.values();
                    final int size = cursor.size();
                    if (randomBoolean()) {
                        for (int doc = 0; doc < size; doc++) {
                            perDoc.get(doc).add(new FieldDescriptor(name, ft, cursor.nextLong(), null));
                        }
                    } else {
                        final long[] vals = new long[size];
                        cursor.fillDocValues(vals, 0, size);
                        for (int doc = 0; doc < size; doc++) {
                            perDoc.get(doc).add(new FieldDescriptor(name, ft, vals[doc], null));
                        }
                    }
                }
            } else if (column instanceof BinaryColumn binaryColumn) {
                if (isSparse || randomBoolean()) {
                    final ObjectTupleCursor<BytesRef> cursor = binaryColumn.tuples();
                    for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
                        perDoc.get(doc).add(new FieldDescriptor(name, ft, null, BytesRef.deepCopyOf(cursor.value())));
                    }
                } else {
                    final BytesRefValuesCursor cursor = binaryColumn.values();
                    for (int doc = 0; doc < cursor.size(); doc++) {
                        perDoc.get(doc).add(new FieldDescriptor(name, ft, null, BytesRef.deepCopyOf(cursor.nextValue())));
                    }
                }
            } else {
                throw new AssertionError("unsupported column type in test harness: " + column.getClass());
            }
        }
    }

    /**
     * Asserts that two field-descriptor lists are equal as ordered-independent multisets (both are
     * sorted before comparison). Subclasses may override to adjust the comparison for known
     * acceptable divergences, but should call {@code super} for the unchanged portion.
     */
    protected void assertFieldSetsEqual(List<FieldDescriptor> expected, List<FieldDescriptor> actual, String message) {
        assertEquals(message, sorted(expected), sorted(actual));
    }

    /**
     * A comparable, value-capturing snapshot of an {@link IndexableField} for order-independent
     * equality comparison between the x-content and columnar paths.
     *
     * <p>The {@code fieldType} is a frozen copy of the original {@link org.apache.lucene.index.IndexableFieldType},
     * compared via {@link FieldType#equals} which covers all structural attributes including
     * {@code docValuesSkipIndexType}. Both paths are expected to produce identical field types.
     *
     * <p>Numeric field values are captured via {@link IndexableField#numericValue()} to avoid the
     * spurious {@code binaryValue()} that {@code ColumnLongField} exposes for doc-values-only fields.
     * String values are normalized to {@link BytesRef} so string and binary representations of the
     * same data compare equal.
     */
    private record FieldDescriptor(String name, FieldType fieldType, @Nullable Long longValue, @Nullable BytesRef bytesValue)
        implements
            Comparable<FieldDescriptor> {

        @Override
        public int compareTo(FieldDescriptor other) {
            int c = name.compareTo(other.name);
            if (c != 0) return c;
            c = fieldType.toString().compareTo(other.fieldType.toString());
            if (c != 0) return c;
            if (longValue != null && other.longValue != null) {
                return Long.compare(longValue, other.longValue);
            }
            if (longValue != null) return 1;
            if (other.longValue != null) return -1;
            if (bytesValue != null && other.bytesValue != null) {
                return bytesValue.compareTo(other.bytesValue);
            }
            if (bytesValue != null) return 1;
            if (other.bytesValue != null) return -1;
            return 0;
        }
    }

    static FieldDescriptor descriptor(IndexableField field) {
        final FieldType ft = new FieldType(field.fieldType());
        ft.freeze();

        final Number numeric = field.numericValue();
        final Long longValue = numeric != null ? numeric.longValue() : null;
        BytesRef bytesValue = null;
        if (longValue == null) {
            final BytesRef binary = field.binaryValue();
            if (binary != null) {
                bytesValue = BytesRef.deepCopyOf(binary);
            } else if (field.stringValue() != null) {
                bytesValue = new BytesRef(field.stringValue());
            }
        }
        return new FieldDescriptor(field.name(), ft, longValue, bytesValue);
    }

    private static IndexRequest[] buildIndexRequests(List<Doc> docs, BytesReference[] sourceBytes) {
        final IndexRequest[] requests = new IndexRequest[docs.size()];
        for (int i = 0; i < docs.size(); i++) {
            final Doc doc = docs.get(i);
            final IndexRequest req = new IndexRequest("test-index").id(doc.id()).source(sourceBytes[i], XContentType.JSON);
            if (doc.routing() != null) {
                req.routing(doc.routing());
            }
            requests[i] = req;
        }
        return requests;
    }

    private static List<FieldDescriptor> toDescriptors(List<IndexableField> fields) {
        final List<FieldDescriptor> descriptors = new ArrayList<>(fields.size());
        for (IndexableField field : fields) {
            descriptors.add(descriptor(field));
        }
        return descriptors;
    }

    private static List<FieldDescriptor> sorted(List<FieldDescriptor> descriptors) {
        return descriptors.stream().sorted(Comparator.naturalOrder()).toList();
    }
}
