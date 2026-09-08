/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.codec.tsdb.ValidatingFieldInfosFormat;

/**
 * The formats Elasticsearch supplies on top of a Lucene codec, so that a codec for a newer Lucene needs only a name and that
 * Lucene codec.
 *
 * <p>Postings, doc values and knn vectors are chosen per field, falling back to whatever the Lucene codec would choose for that
 * field. Field infos are shared across a shard's segments and validate synthetic ids, stored fields select their implementation
 * per segment, and points size their BKD leaves from the data. The remaining formats come from the Lucene codec unchanged.
 *
 * <p>The name matters: a codec is resolved from the name recorded in the segment, and of the eleven formats a codec supplies only
 * postings, doc values and knn vectors are {@code NamedSPI} and recorded per field. The name selects the other eight, so a codec
 * whose name-selected formats differ from an existing one needs a name of its own.
 */
public abstract class ElasticsearchCodec extends FilterCodec {

    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return ElasticsearchCodec.this.getPostingsFormatForField(field);
        }
    };

    private final DocValuesFormat docValuesFormat = new XPerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return ElasticsearchCodec.this.getDocValuesFormatForField(field);
        }
    };

    private final KnnVectorsFormat knnVectorsFormat = new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return ElasticsearchCodec.this.getKnnVectorsFormatForField(field);
        }
    };

    private final PerFieldPostingsFormat delegatePostingsFormat;
    private final PerFieldDocValuesFormat delegateDocValuesFormat;
    private final PerFieldKnnVectorsFormat delegateKnnVectorsFormat;
    private final FieldInfosFormat fieldInfosFormat;
    private final StoredFieldsFormat storedFieldsFormat;

    /**
     * @param name             the name recorded in segments this codec writes
     * @param delegate         the Lucene codec supplying the formats this one does not
     * @param storedFieldsMode the stored fields implementation segments are written with
     * @param legacyMode       what a segment recording no stored fields mode was written with, which depends on the name it carries
     * @param syntheticId      whether segments written through this codec must carry a synthetic id
     */
    @SuppressWarnings("this-escape")
    protected ElasticsearchCodec(
        String name,
        Codec delegate,
        ElasticsearchStoredFieldsFormat.Mode storedFieldsMode,
        ElasticsearchStoredFieldsFormat.Mode legacyMode,
        boolean syntheticId
    ) {
        super(name, delegate);
        this.delegatePostingsFormat = perField(delegate.postingsFormat(), PerFieldPostingsFormat.class, name);
        this.delegateDocValuesFormat = perField(delegate.docValuesFormat(), PerFieldDocValuesFormat.class, name);
        this.delegateKnnVectorsFormat = perField(delegate.knnVectorsFormat(), PerFieldKnnVectorsFormat.class, name);
        // TODO: the synthetic id pieces below apply to every codec even though only time series indices use them. Moving them
        // into a subclass means reworking how PerFieldMapperCodec is built, so it is left for a follow-up.
        this.fieldInfosFormat = new ElasticsearchFieldInfosFormat(new ValidatingFieldInfosFormat(delegate.fieldInfosFormat(), syntheticId));
        // TSDBStoredFieldsFormat adds a reader for synthetic ids, and only for segments whose _id says it has one; writes go
        // straight to the format underneath. Segments without a synthetic id are unaffected either way.
        this.storedFieldsFormat = new TSDBStoredFieldsFormat(
            new ElasticsearchStoredFieldsFormat(storedFieldsMode, legacyMode, delegate.storedFieldsFormat())
        );
    }

    private static <T> T perField(Object format, Class<T> type, String name) {
        if (type.isInstance(format) == false) {
            throw new IllegalArgumentException(
                "codec [" + name + "] delegates to a codec whose " + type.getSimpleName() + " does not dispatch per field: " + format
            );
        }
        return type.cast(format);
    }

    /** The Lucene codec this one delegates to. */
    public final Codec delegate() {
        return delegate;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

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
        return delegatePostingsFormat.getPostingsFormatForField(field);
    }

    /** Doc values format for writing new segments of {@code field}; subclasses dispatch per field. */
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return delegateDocValuesFormat.getDocValuesFormatForField(field);
    }

    /** Vectors format for writing new segments of {@code field}; subclasses dispatch per field. */
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return delegateKnnVectorsFormat.getKnnVectorsFormatForField(field);
    }
}
