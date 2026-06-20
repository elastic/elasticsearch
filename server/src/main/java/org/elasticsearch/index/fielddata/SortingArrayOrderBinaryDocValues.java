/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.FieldArrayContext;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;

import java.io.IOException;

/**
 * Reader for the high-cardinality columnar {@code ArrayOrderInlineNull} binary doc-values format, where values are stored in document
 * (array) order with inline {@code null} markers and no {@code .offsets} sidecar. Per document the on-disk layout is:
 * <ul>
 *   <li>{@code .counts} absent &rarr; the field is absent for this document</li>
 *   <li>{@code .counts} present, binary absent &rarr; an all-null or empty array: zero non-null values to expose</li>
 *   <li>{@code .counts == 1}, binary present &rarr; a single non-null value stored raw (both layouts)</li>
 *   <li>{@code .counts >= 2}, binary present, standard layout &rarr; {@code [len+1][val]...} slots,
 *       where {@code len+1 == 0} marks a (dropped) null</li>
 *   <li>{@code .counts >= 2}, binary present, deduplicating layout &rarr;
 *       {@code [D][len1][val1]...[lenD][valD][ord1]...} where ordinal 0 = null (dropped), k&ge;1 = distinct value k-1</li>
 * </ul>
 * The reader advances on {@code .counts} so an all-null or empty array (counts present, binary absent) is handled correctly.
 * <p>
 * When {@code deduplicated} is {@code true}, the deduplicating blob layout is assumed; otherwise the standard layout is used.
 * Null values are always dropped — the returned values are sorted (matching {@link org.apache.lucene.index.SortedSetDocValues}
 * per-doc set semantics).
 */
public final class SortingArrayOrderBinaryDocValues extends SortingBinaryDocValues {

    private final BinaryDocValues binary;
    private final NumericDocValues counts;
    private final boolean deduplicated;
    private final ByteArrayStreamInput in = new ByteArrayStreamInput();

    public SortingArrayOrderBinaryDocValues(BinaryDocValues binary, NumericDocValues counts) {
        this(binary, counts, false);
    }

    public SortingArrayOrderBinaryDocValues(BinaryDocValues binary, NumericDocValues counts, boolean deduplicated) {
        this.binary = binary;
        this.counts = counts;
        this.deduplicated = deduplicated;
    }

    public static SortingArrayOrderBinaryDocValues from(LeafReader leafReader, String valuesFieldName) throws IOException {
        return from(leafReader, valuesFieldName, false);
    }

    public static SortingArrayOrderBinaryDocValues from(LeafReader leafReader, String valuesFieldName, boolean deduplicated)
        throws IOException {
        // ArrayOrder expects no offsets to be stored
        assert leafReader.getSortedDocValues(FieldArrayContext.offsetsFieldName(valuesFieldName)) == null
            : "ArrayOrderInlineNull field [" + valuesFieldName + "] must not have an .offsets sidecar";
        BinaryDocValues binary = DocValues.getBinary(leafReader, valuesFieldName);
        String countsFieldName = valuesFieldName + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;
        NumericDocValues counts = leafReader.getNumericDocValues(countsFieldName);
        return new SortingArrayOrderBinaryDocValues(binary, counts, deduplicated);
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        // No values
        if (counts == null || counts.advanceExact(doc) == false) {
            count = 0;
            return false;
        }

        int slotCount = Math.toIntExact(counts.longValue());

        // all-null array (slotCount nulls) or empty array (slotCount == 0): no non-null values to iterate
        if (binary.advanceExact(doc) == false) {
            count = 0;
            return false;
        }

        BytesRef bytes = binary.binaryValue();

        if (slotCount == 1) {
            // single non-null value stored raw (a lone null writes no binary blob, handled by the binary-absent branch above)
            count = 1;
            grow();
            values[0].copyBytes(bytes.bytes, bytes.offset, bytes.length);
            sort();
            return true;
        }

        if (deduplicated) {
            return advanceExactDeduplicated(bytes, slotCount);
        }

        // two or more slots (standard layout): [len+1][val]..., where len+1 == 0 marks a null we drop. Size the scratch to the slot
        // count (an upper bound on the surviving non-null values) before decoding, then trim the count to the non-null total.
        count = slotCount;
        grow();
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        int nonNull = 0;
        for (int i = 0; i < slotCount; i++) {
            int encodedLength = in.readVInt();
            if (encodedLength == 0) {
                continue; // null slot
            }
            int length = encodedLength - 1;
            int offset = in.getPosition();
            values[nonNull].copyBytes(bytes.bytes, offset, length);
            in.setPosition(offset + length);
            nonNull++;
        }
        count = nonNull;
        sort();
        return nonNull > 0;
    }

    /**
     * Decodes the deduplicating layout ({@code [D][len/val x D][ordinal x slotCount]}) and populates the sorted scratch values.
     * Null ordinals (0) are dropped; the distinct values are expanded per ordinal and then sorted.
     */
    private boolean advanceExactDeduplicated(BytesRef bytes, int slotCount) throws IOException {
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        int D = in.readVInt();

        // Read the D distinct value offsets and lengths within bytes.bytes.
        int[] distinctOffsets = new int[D];
        int[] distinctLengths = new int[D];
        for (int d = 0; d < D; d++) {
            int length = in.readVInt();
            int offset = in.getPosition();
            in.setPosition(offset + length);
            distinctOffsets[d] = offset;
            distinctLengths[d] = length;
        }

        // Expand ordinals to values (drop nulls).
        count = slotCount;
        grow();
        int nonNull = 0;
        for (int i = 0; i < slotCount; i++) {
            int ord = in.readVInt();
            if (ord == 0) {
                continue; // null slot
            }
            int d = ord - 1;
            values[nonNull].copyBytes(bytes.bytes, distinctOffsets[d], distinctLengths[d]);
            nonNull++;
        }
        count = nonNull;
        sort();
        return nonNull > 0;
    }
}
