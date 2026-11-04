/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Walks the column chunks of a flat column over the row groups of the pieces of a segment, and makes the page of a document current.
 * A document is a row, so the document {@code d} is the {@code d}-th row of the column over all the row groups.
 *
 * <p>A chunk is a dictionary page, if the column has one, followed by data pages in row order. There is no index to find the page of a
 * row, so the reader walks the page headers from the start of the chunk and skips the pages that end before the target without
 * decompressing them. A {@link PageDecoder} turns the bytes of a page into values. For an {@code OPTIONAL} column the reader decodes
 * the definition levels itself, and hands the decoder only the values that are present.
 *
 * <p>Not thread safe: an iterator owns one reader.
 */
final class ColumnChunkReader {

    /** One column chunk of one row group, with the position of its first document in the segment. */
    record Block(int startDoc, int numRows, ParquetObject object, long offset, long compressedSize, int codec) {}

    /** Decodes the values of the pages of one column. */
    interface PageDecoder {

        /** A new chunk starts, so the dictionary of the previous chunk does not apply any more. */
        void startChunk();

        /** Decodes the dictionary page of the chunk. The decoder may keep {@code bytes}, which belong to it. */
        void dictionary(byte[] bytes, int offset, int end, int numValues) throws IOException;

        /**
         * Decodes the values of a data page. The decoder may keep {@code bytes}, which belong to it.
         *
         * @param numValues the number of rows of the page, which is also the number of entries of {@code present}
         * @param nonNull the number of values that are encoded in {@code bytes[offset, end)}
         * @param present for each row whether it has a value, or {@code null} when every row has one
         */
        void page(byte[] bytes, int offset, int end, int numValues, int nonNull, boolean[] present, Encoding encoding) throws IOException;
    }

    private final List<Block> blocks;
    private final int[] blockStarts;
    private final int maxDoc;
    private final boolean required;
    private final PageDecoder decoder;

    // the chunk that is being read
    private int blockIndex = -1;
    private byte[] chunk;
    private int chunkPosition;
    // the page that is current: rows of the block, and whether it was decoded or only passed
    private int pageStartRow;
    private int pageRows;
    private boolean pageDecoded;
    private boolean[] present;

    ColumnChunkReader(List<Block> blocks, boolean required, PageDecoder decoder) {
        this.blocks = blocks;
        this.blockStarts = blocks.stream().mapToInt(Block::startDoc).toArray();
        this.maxDoc = blocks.isEmpty() ? 0 : blocks.get(blocks.size() - 1).startDoc() + blocks.get(blocks.size() - 1).numRows();
        this.required = required;
        this.decoder = decoder;
    }

    int maxDoc() {
        return maxDoc;
    }

    /** The document of the first row of the current page. */
    int pageStartDoc() {
        return blocks.get(blockIndex).startDoc() + pageStartRow;
    }

    /** The number of rows of the current page. */
    int pageRows() {
        return pageRows;
    }

    /** Whether the document has a value. The page of the document must be current. */
    boolean present(int doc) {
        return present == null || present[doc - pageStartDoc()];
    }

    /** Makes the page that holds the document current, decoding it if it is not. */
    void load(int doc) throws IOException {
        int index = Arrays.binarySearch(blockStarts, doc);
        if (index < 0) {
            index = -index - 2;
        }
        Block block = blocks.get(index);
        int row = doc - block.startDoc();
        boolean insideSkippedPage = pageDecoded == false && row < pageStartRow + pageRows;
        if (index != blockIndex || row < pageStartRow || insideSkippedPage) {
            // a new chunk, or a seek backward, which starts again from the first page of the chunk
            startChunk(index, block);
        } else if (pageDecoded && row < pageStartRow + pageRows) {
            return;
        }
        while (pageDecoded == false || row >= pageStartRow + pageRows) {
            nextPage(block, row);
        }
    }

    private void startChunk(int index, Block block) throws IOException {
        blockIndex = index;
        chunk = block.object().readRange(block.offset(), Math.toIntExact(block.compressedSize()));
        chunkPosition = 0;
        pageStartRow = 0;
        pageRows = 0;
        pageDecoded = false;
        present = null;
        decoder.startChunk();
    }

    /** Moves to the next data page of the chunk: decodes it if it holds {@code row}, and passes it if it ends before. */
    private void nextPage(Block block, int row) throws IOException {
        if (chunkPosition >= chunk.length) {
            throw new IOException("the chunk at [" + block.offset() + "] ends before row [" + row + "]");
        }
        ByteArrayInputStream in = new ByteArrayInputStream(chunk, chunkPosition, chunk.length - chunkPosition);
        PageHeader header = Util.readPageHeader(in);
        int headerLength = (chunk.length - chunkPosition) - in.available();
        int bodyStart = chunkPosition + headerLength;
        int compressedSize = header.getCompressed_page_size();
        if (bodyStart + compressedSize > chunk.length) {
            throw new IOException("a page of the chunk at [" + block.offset() + "] is cut off");
        }
        chunkPosition = bodyStart + compressedSize;

        PageType type = header.getType();
        if (type == PageType.DICTIONARY_PAGE) {
            byte[] values = PageDecompressor.decompress(
                block.codec(),
                chunk,
                bodyStart,
                compressedSize,
                header.getUncompressed_page_size()
            );
            decoder.dictionary(values, 0, values.length, header.getDictionary_page_header().getNum_values());
            return;
        }
        final int numValues;
        final Encoding encoding;
        if (type == PageType.DATA_PAGE) {
            numValues = header.getData_page_header().getNum_values();
            encoding = header.getData_page_header().getEncoding();
        } else if (type == PageType.DATA_PAGE_V2) {
            numValues = header.getData_page_header_v2().getNum_values();
            encoding = header.getData_page_header_v2().getEncoding();
        } else {
            // an index page or an unknown page: not data, so it does not move the row
            return;
        }
        pageStartRow = pageStartRow + pageRows;
        pageRows = numValues;
        pageDecoded = false;
        present = null;
        if (row >= pageStartRow + pageRows) {
            return; // the target is in a later page: pass this one without decoding it
        }
        decodePage(block, header, bodyStart, compressedSize, numValues, encoding);
        pageDecoded = true;
    }

    private void decodePage(Block block, PageHeader header, int bodyStart, int compressedSize, int numValues, Encoding encoding)
        throws IOException {
        final byte[] values;
        final int valuesOffset;
        int nonNull = numValues;
        boolean[] presentRows = null;
        if (header.getType() == PageType.DATA_PAGE) {
            values = PageDecompressor.decompress(block.codec(), chunk, bodyStart, compressedSize, header.getUncompressed_page_size());
            if (required) {
                valuesOffset = 0;
            } else {
                // the levels come first, as the 4-byte little-endian length of the encoded levels and then the levels
                int levelsLength = (values[0] & 0xFF) | ((values[1] & 0xFF) << 8) | ((values[2] & 0xFF) << 16) | ((values[3] & 0xFF) << 24);
                if (levelsLength < 0 || 4 + levelsLength > values.length) {
                    throw new IOException("the definition levels of a page are cut off");
                }
                presentRows = new boolean[numValues];
                nonNull = decodeLevels(values, 4, 4 + levelsLength, numValues, presentRows);
                valuesOffset = 4 + levelsLength;
            }
            decoder.page(values, valuesOffset, values.length, numValues, nonNull, presentRows, encoding);
        } else {
            var v2 = header.getData_page_header_v2();
            int definitionLength = v2.getDefinition_levels_byte_length();
            int repetitionLength = v2.getRepetition_levels_byte_length();
            if (repetitionLength != 0) {
                throw new IOException("a flat column has no repetition levels, but a page has some");
            }
            if (required && definitionLength != 0) {
                throw new IOException("a required column has no definition levels, but a page has some");
            }
            if (required == false) {
                presentRows = new boolean[numValues];
                nonNull = decodeLevels(chunk, bodyStart, bodyStart + definitionLength, numValues, presentRows);
            }
            // only the values of a version 2 page are compressed, the levels in front of them are not
            int valuesStart = bodyStart + definitionLength;
            int valuesCompressed = compressedSize - definitionLength;
            int valuesUncompressed = header.getUncompressed_page_size() - definitionLength;
            values = v2.isIs_compressed()
                ? PageDecompressor.decompress(block.codec(), chunk, valuesStart, valuesCompressed, valuesUncompressed)
                : Arrays.copyOfRange(chunk, valuesStart, valuesStart + valuesCompressed);
            decoder.page(values, 0, values.length, numValues, nonNull, presentRows, encoding);
        }
        present = presentRows;
    }

    /** Decodes the definition levels of an optional flat column (0 or 1, so one bit wide) into {@code present}, and counts the 1s. */
    private static int decodeLevels(byte[] bytes, int offset, int end, int numValues, boolean[] present) throws IOException {
        int[] levels = new int[numValues];
        RleBitPackedDecoder.decode(bytes, offset, end, 1, levels, numValues);
        int nonNull = 0;
        for (int i = 0; i < numValues; i++) {
            if (levels[i] == 1) {
                present[i] = true;
                nonNull++;
            } else if (levels[i] != 0) {
                throw new IOException("a definition level [" + levels[i] + "] is above the maximum of a flat column");
            }
        }
        return nonNull;
    }
}
