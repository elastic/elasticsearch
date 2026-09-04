/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * Elasticsearch-named counterpart of Lucene's {@link Lucene104Codec}, backing {@link DefaultCompressionPerFieldMapperCodec} and so
 * {@code index.codec=default}.
 *
 * <p>Its only reason to exist is the codec name. A codec is resolved on the read path by the name recorded in the segment
 * ({@code SegmentInfos.readCommit} calls {@code Codec.forName}), so a codec that inherits Lucene's own name resolves back to Lucene's
 * plain codec and never sees the {@link CodecService.DeduplicateFieldInfosCodec} wrapper that {@link CodecService} applies when writing.
 * Field infos then get neither the per-directory instance cache nor the node-wide name and attribute interning, and every segment holds
 * its own copies — costly for mappings with many fields, and invisible on the write path, which does go through the wrapper.
 *
 * <p>The name deliberately carries no Lucene version and no compression: of the eleven formats a codec supplies, only postings, doc
 * values and knn vectors are {@code NamedSPI} and recorded per field, so this name is the sole thing selecting the other eight on read.
 * That makes it the compatibility lever, which is why it is versioned, and why it should not be narrowed to describe today's stored-fields
 * choice — the intent is for it to cover {@code best_compression} too once stored fields become self-describing.
 *
 * <p>Formats are inherited from the delegate except the three per-field ones: Lucene's own dispatch calls back into the delegate rather
 * than this codec, so they are re-declared here to route through {@link #getPostingsFormatForField} and friends. They must stay
 * byte-identical to Lucene's; {@code CodecTests} asserts that.
 */
public class Elasticsearch96Codec extends CodecService.DeduplicateFieldInfosCodec {

    private final PostingsFormat defaultPostingsFormat = new Lucene104PostingsFormat();
    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return Elasticsearch96Codec.this.getPostingsFormatForField(field);
        }
    };

    private final DocValuesFormat defaultDVFormat = new Lucene90DocValuesFormat();
    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return Elasticsearch96Codec.this.getDocValuesFormatForField(field);
        }
    };

    private final KnnVectorsFormat defaultKnnVectorsFormat = new Lucene99HnswVectorsFormat();
    private final KnnVectorsFormat knnVectorsFormat = new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return Elasticsearch96Codec.this.getKnnVectorsFormatForField(field);
        }
    };

    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public Elasticsearch96Codec() {
        this(Lucene104Codec.Mode.BEST_SPEED);
    }

    public Elasticsearch96Codec(Lucene104Codec.Mode mode) {
        super("Elasticsearch96", new Lucene104Codec(mode));
    }

    @Override
    public final PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        return knnVectorsFormat;
    }

    /** Postings format for writing new segments of {@code field}; subclasses dispatch per field. */
    public PostingsFormat getPostingsFormatForField(String field) {
        return defaultPostingsFormat;
    }

    /** Doc values format for writing new segments of {@code field}; subclasses dispatch per field. */
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return defaultDVFormat;
    }

    /** Vectors format for writing new segments of {@code field}; subclasses dispatch per field. */
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return defaultKnnVectorsFormat;
    }
}
