/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serves doc values of a segment from external Parquet files. The same class backs two uses: the segment that Lucene opens (built from
 * the meta file by the format) and the reader that attaches a new stub segment (built from a file description).
 *
 * <p>Numeric fields are decoded from the Parquet files by {@link ParquetNumericDocValues}.
 */
public final class ParquetDocValuesProducer extends DocValuesProducer {

    private final Map<Integer, ColumnBinding> bindingsByFieldNumber;
    private final List<ExternalFilePiece> pieces;
    private final int maxDoc;
    /** The file of each piece, in the order of the pieces, or {@code null} for a producer that cannot read values. */
    @Nullable
    private final List<ParquetObject> files;

    /**
     * @param objects opens the files, or {@code null} for a producer that only describes a segment and cannot read a value
     */
    public ParquetDocValuesProducer(List<ColumnBinding> bindings, List<ExternalFilePiece> pieces, @Nullable ParquetObjects objects)
        throws IOException {
        // The files are opened now, while the segment is being opened, and not when a value is read. Opening a repository file needs
        // the blob store of the repository, which must not be created on a search thread, and a read on a search thread only needs
        // the container that is already open here.
        if (objects == null) {
            this.files = null;
        } else {
            List<ParquetObject> opened = new ArrayList<>(pieces.size());
            for (ExternalFilePiece piece : pieces) {
                opened.add(objects.open(piece.location(), piece.length()));
            }
            this.files = List.copyOf(opened);
        }
        this.bindingsByFieldNumber = bindings.stream().collect(Collectors.toMap(ColumnBinding::fieldNumber, b -> b));
        this.pieces = List.copyOf(pieces);
        long total = 0;
        for (ExternalFilePiece piece : pieces) {
            total += piece.maxDoc();
        }
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("the pieces hold [" + total + "] rows, more than a segment can hold");
        }
        this.maxDoc = (int) total;
    }

    public List<ColumnBinding> bindings() {
        return List.copyOf(bindingsByFieldNumber.values());
    }

    public List<ExternalFilePiece> pieces() {
        return pieces;
    }

    public int maxDoc() {
        return maxDoc;
    }

    /** The chunks of a column over the pieces of the segment, and the snapshot of the column in the first piece. */
    private record ColumnBlocks(List<ColumnChunkReader.Block> blocks, ColumnSnapshot column) {}

    private ColumnBlocks blocksOf(ColumnBinding binding) throws IOException {
        List<ColumnChunkReader.Block> blocks = new ArrayList<>();
        ColumnSnapshot typeSource = null;
        int startDoc = 0;
        for (int pieceIndex = 0; pieceIndex < pieces.size(); pieceIndex++) {
            ExternalFilePiece piece = pieces.get(pieceIndex);
            ColumnSnapshot column = piece.columns()
                .stream()
                .filter(c -> c.path().equals(binding.parquetColumnPath()))
                .findFirst()
                .orElseThrow(() -> new IOException("the segment has no snapshot of column [" + binding.parquetColumnPath() + "]"));
            if (PageDecompressor.supports(column.codec()) == false) {
                throw new UnsupportedOperationException("compression codec [" + column.codec() + "] is not supported yet");
            }
            if (files == null) {
                throw new IllegalStateException(
                    "this segment was not opened by an engine that knows where its Parquet files are, so it cannot read values"
                );
            }
            ParquetObject object = files.get(pieceIndex);
            typeSource = typeSource == null ? column : typeSource;
            for (int rowGroup = 0; rowGroup < piece.rowGroupRows().length; rowGroup++) {
                int rows = piece.rowGroupRows()[rowGroup];
                if (rows == 0) {
                    continue;
                }
                // a flat column has one entry in a chunk for each row, a missing value included
                if (column.numValues()[rowGroup] != rows) {
                    throw new IOException(
                        "column [" + column.path() + "] has [" + column.numValues()[rowGroup] + "] values for [" + rows + "] rows"
                    );
                }
                blocks.add(
                    new ColumnChunkReader.Block(
                        startDoc,
                        rows,
                        object,
                        column.offsets()[rowGroup],
                        column.compressedSizes()[rowGroup],
                        column.codec()
                    )
                );
                startDoc += rows;
            }
        }
        return new ColumnBlocks(blocks, typeSource);
    }

    private ColumnBinding bindingOf(FieldInfo field) {
        ColumnBinding binding = bindingsByFieldNumber.get(field.number);
        if (binding == null) {
            throw new IllegalArgumentException("field [" + field.name + "] is not served from Parquet");
        }
        return binding;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        ColumnBinding binding = bindingOf(field);
        ColumnBlocks source = blocksOf(binding);
        if (source.column() == null || source.blocks().isEmpty()) {
            return DocValues.emptyNumeric();
        }
        ColumnSnapshot column = source.column();
        return new ParquetNumericDocValues(
            source.blocks(),
            column.required(),
            column.physicalType(),
            column.convertedType(),
            binding.conversion()
        );
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        ColumnBinding binding = bindingOf(field);
        if (binding.isSynthesized()) {
            return new RowNumberId();
        }
        ColumnBlocks source = blocksOf(binding);
        if (source.column() == null || source.blocks().isEmpty()) {
            return DocValues.emptyBinary();
        }
        return new ParquetBinaryDocValues(source.blocks(), source.column().required());
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        throw new UnsupportedOperationException("ordinals are not part of Goal 1");
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
        throw new UnsupportedOperationException("sorted numeric doc values are not part of the attach spike");
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        throw new UnsupportedOperationException("ordinals are not part of Goal 1");
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) {
        ColumnBinding binding = bindingOf(field);
        if (binding.isSynthesized() || binding.type() != DocValuesType.NUMERIC) {
            return null;
        }
        return ParquetDocValuesSkipper.of(binding, pieces);
    }

    @Override
    public void checkIntegrity() {}

    @Override
    public void close() {}

    /**
     * The {@code _id} of a document: the row number in the index as a string, encoded as the mapper encodes an {@code _id}. The row
     * number is the row of the document in its piece plus the number of rows of the pieces before it in the index.
     */
    private final class RowNumberId extends BinaryDocValues implements BlockLoader.OptionalColumnAtATimeReader {
        private int doc = -1;
        private int piece = 0;
        private int pieceFirstDoc = 0;

        @Override
        public BytesRef binaryValue() {
            return Uid.encodeId(Long.toString(pieces.get(piece).indexRowBase() + (doc - pieceFirstDoc)));
        }

        @Override
        public boolean advanceExact(int target) {
            if (target >= maxDoc) {
                doc = DocIdSetIterator.NO_MORE_DOCS;
                return false;
            }
            if (target < pieceFirstDoc) {
                piece = 0;
                pieceFirstDoc = 0;
            }
            while (target >= pieceFirstDoc + pieces.get(piece).maxDoc()) {
                pieceFirstDoc += pieces.get(piece).maxDoc();
                piece++;
            }
            doc = target;
            return true;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            if (advanceExact(target) == false) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            return doc;
        }

        @Override
        public long cost() {
            return maxDoc;
        }

        @Override
        public BlockLoader.Block tryRead(
            BlockLoader.BlockFactory factory,
            BlockLoader.Docs docs,
            int offset,
            boolean nullsFiltered,
            BlockDocValuesReader.ToDouble toDouble,
            boolean toInt,
            boolean binaryMultiValuedFormat
        ) {
            return null;
        }

        /**
         * An {@code _id} query finds its document without a scan: the id is a row number, so the document is at a known position of
         * one piece, or there is none.
         */
        @Override
        public DocIdSetIterator tryTermEqualIterator(BytesRef term) {
            final String id;
            final long row;
            try {
                id = Uid.decodeId(term.bytes, term.offset, term.length);
                row = Long.parseLong(id);
            } catch (RuntimeException e) {
                // not the encoding of an id, or not a number: no row has this id
                return DocIdSetIterator.empty();
            }
            // "05" and "+5" are not the id of row 5
            if (Long.toString(row).equals(id) == false) {
                return DocIdSetIterator.empty();
            }
            int firstDocOfPiece = 0;
            for (ExternalFilePiece piece : pieces) {
                if (row >= piece.indexRowBase() && row - piece.indexRowBase() < piece.maxDoc()) {
                    int docId = firstDocOfPiece + (int) (row - piece.indexRowBase());
                    return DocIdSetIterator.range(docId, docId + 1);
                }
                firstDocOfPiece += piece.maxDoc();
            }
            return DocIdSetIterator.empty();
        }
    }
}
