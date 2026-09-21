/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.lucene87;

import org.apache.lucene.backward_codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.elasticsearch.index.codec.lucene50.Lucene50CompoundFormat;
import org.elasticsearch.index.codec.lucene50.Lucene50LiveDocsFormat;
import org.elasticsearch.index.codec.lucene50.Lucene50TermVectorsFormat;
import org.elasticsearch.index.codec.lucene60.Lucene60FieldInfosFormat;
import org.elasticsearch.index.codec.lucene80.Lucene80NormsFormat;
import org.elasticsearch.index.codec.lucene84.Lucene84PostingsFormat;
import org.elasticsearch.index.codec.lucene86.Lucene86PointsFormat;
import org.elasticsearch.index.codec.lucene86.Lucene86SegmentInfoFormat;

import java.util.Objects;

/**
 * Implements the Lucene 8.6 index format, with configurable per-field postings and docvalues
 * formats.
 *
 * <p>If you want to reuse functionality of this codec in another codec, extend {@link FilterCodec}.
 *
 */
public class Lucene87Codec extends Codec {

    /** Configuration option for the codec. */
    public enum Mode {
        /** Trade compression ratio for retrieval speed. */
        BEST_SPEED(Lucene87StoredFieldsFormat.Mode.BEST_SPEED, Lucene80DocValuesFormat.Mode.BEST_SPEED),
        /** Trade retrieval speed for compression ratio. */
        BEST_COMPRESSION(Lucene87StoredFieldsFormat.Mode.BEST_COMPRESSION, Lucene80DocValuesFormat.Mode.BEST_COMPRESSION);

        /** compression mode for stored fields */
        protected final Lucene87StoredFieldsFormat.Mode storedMode;

        /** compression mode for doc value fields */
        protected final Lucene80DocValuesFormat.Mode dvMode;

        private Mode(Lucene87StoredFieldsFormat.Mode storedMode, Lucene80DocValuesFormat.Mode dvMode) {
            this.storedMode = Objects.requireNonNull(storedMode);
            this.dvMode = Objects.requireNonNull(dvMode);
        }
    }

    private final TermVectorsFormat vectorsFormat = new Lucene50TermVectorsFormat();
    private final FieldInfosFormat fieldInfosFormat = new Lucene60FieldInfosFormat();
    private final SegmentInfoFormat segmentInfosFormat = new Lucene86SegmentInfoFormat();
    private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();
    private final PointsFormat pointsFormat = new Lucene86PointsFormat();
    private final PostingsFormat defaultFormat;

    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return Lucene87Codec.this.getPostingsFormatForField(field);
        }
    };

    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return Lucene87Codec.this.getDocValuesFormatForField(field);
        }
    };

    private final StoredFieldsFormat storedFieldsFormat;

    /** Instantiates a new codec. */
    public Lucene87Codec() {
        this(Mode.BEST_COMPRESSION);
    }

    /**
     * Instantiates a new codec, specifying the compression mode to use.
     *
     * @param mode compression mode to use for newly flushed/merged segments.
     */
    public Lucene87Codec(Mode mode) {
        super("Lucene87");
        this.storedFieldsFormat = new Lucene87StoredFieldsFormat(mode.storedMode);
        this.defaultFormat = new Lucene84PostingsFormat();
        this.defaultDVFormat = new Lucene80DocValuesFormat(mode.dvMode);
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return vectorsFormat;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public final LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public PointsFormat pointsFormat() {
        return pointsFormat;
    }

    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        return KnnVectorsFormat.EMPTY;
    }

    /**
     * Returns the postings format that should be used for writing new segments of <code>field</code>.
     *
     * <p>The default implementation always returns "Lucene84".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation.
     */
    public PostingsFormat getPostingsFormatForField(String field) {
        return defaultFormat;
    }

    /**
     * Returns the docvalues format that should be used for writing new segments of <code>field</code>
     * .
     *
     * <p>The default implementation always returns "Lucene80".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation.
     */
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return defaultDVFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    private final DocValuesFormat defaultDVFormat;

    private final NormsFormat normsFormat = new Lucene80NormsFormat();

    @Override
    public NormsFormat normsFormat() {
        return normsFormat;
    }
}
