/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * Elasticsearch-named counterpart of Lucene's {@link Lucene104Codec}, backing {@link PerFieldMapperCodec} and so
 * {@code index.codec=default}.
 *
 * <p>The name is what makes the codec's own formats reachable on read: a codec is resolved from the name recorded in the segment,
 * and of the eleven formats a codec supplies only postings, doc values and knn vectors are {@code NamedSPI} and recorded per field.
 * This name selects the other eight.
 *
 * <p>Postings, doc values and knn vectors are chosen per field. Field infos are shared across a shard's segments, stored fields are
 * selected per segment, and points size their BKD leaves from the data. The rest come from the Lucene codec unchanged.
 */
public class Elasticsearch96Codec extends FilterCodec {

    private final PostingsFormat defaultPostingsFormat = new Lucene104PostingsFormat();
    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return Elasticsearch96Codec.this.getPostingsFormatForField(field);
        }
    };

    private final DocValuesFormat defaultDVFormat = new Lucene90DocValuesFormat();
    private final DocValuesFormat docValuesFormat = new XPerFieldDocValuesFormat() {
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

    private final FieldInfosFormat fieldInfosFormat;

    /** Shares field infos across the segments of a shard. */
    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    /** The Lucene codec this one delegates to. */
    public final Codec delegate() {
        return delegate;
    }

    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public Elasticsearch96Codec() {
        this(Lucene104Codec.Mode.BEST_SPEED);
    }

    public Elasticsearch96Codec(Lucene104Codec.Mode mode) {
        this(mode, ElasticsearchStoredFieldsFormat.Mode.LUCENE);
    }

    /**
     * @param luceneMode      the Lucene compression level, used when {@code storedFieldsMode} is
     *                        {@link ElasticsearchStoredFieldsFormat.Mode#LUCENE}
     * @param storedFieldsMode the stored fields implementation segments are written with
     */
    public Elasticsearch96Codec(Lucene104Codec.Mode luceneMode, ElasticsearchStoredFieldsFormat.Mode storedFieldsMode) {
        this(luceneMode, storedFieldsMode, ElasticsearchStoredFieldsFormat.Mode.LUCENE);
    }

    /**
     * @param modeBeforeTheAttribute what a segment recording no stored fields mode was written with, which depends on the codec
     *                               name the segment carries
     */
    public Elasticsearch96Codec(
        Lucene104Codec.Mode luceneMode,
        ElasticsearchStoredFieldsFormat.Mode storedFieldsMode,
        ElasticsearchStoredFieldsFormat.Mode modeBeforeTheAttribute
    ) {
        super("Elasticsearch96", new Lucene104Codec(luceneMode));
        this.fieldInfosFormat = new ElasticsearchFieldInfosFormat(delegate.fieldInfosFormat());
        this.storedFieldsFormat = new ElasticsearchStoredFieldsFormat(
            storedFieldsMode,
            modeBeforeTheAttribute,
            delegate.storedFieldsFormat()
        );
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    private final StoredFieldsFormat storedFieldsFormat;

    /**
     * Sizes BKD leaves from the data rather than using a fixed maximum. The on-disk format is unchanged and the reader is Lucene's,
     * so segments written either way are readable by both.
     */
    @Override
    public final PointsFormat pointsFormat() {
        return Elasticsearch900AdaptivePointsFormat.INSTANCE;
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
