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
package org.elasticsearch.index.codec.lucene80;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
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
import org.elasticsearch.index.codec.lucene50.Lucene50PostingsFormat;
import org.elasticsearch.index.codec.lucene50.Lucene50StoredFieldsFormat;
import org.elasticsearch.index.codec.lucene50.Lucene50TermVectorsFormat;
import org.elasticsearch.index.codec.lucene60.Lucene60FieldInfosFormat;
import org.elasticsearch.index.codec.lucene60.Lucene60PointsFormat;
import org.elasticsearch.index.codec.lucene70.Lucene70SegmentInfoFormat;

/**
 * Implements the Lucene 8.0 index format.
 *
 * @see org.apache.lucene.backward_codecs.lucene80 package documentation for file format details.
 * @lucene.experimental
 */
public class Lucene80Codec extends Codec {
    private final TermVectorsFormat vectorsFormat = new Lucene50TermVectorsFormat();
    private final FieldInfosFormat fieldInfosFormat = new Lucene60FieldInfosFormat();
    private final SegmentInfoFormat segmentInfosFormat = new Lucene70SegmentInfoFormat();
    private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();

    private final PostingsFormat defaultFormat = new Lucene50PostingsFormat();

    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return defaultFormat;
        }
    };

    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return defaultDVFormat;
        }
    };
    private final DocValuesFormat defaultDVFormat = DocValuesFormat.forName("Lucene80");

    private final StoredFieldsFormat storedFieldsFormat;

    /** Instantiates a new codec. */
    public Lucene80Codec() {
        super("Lucene80");
        this.storedFieldsFormat = new Lucene50StoredFieldsFormat();
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return vectorsFormat;
    }

    @Override
    public final PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public final SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public final LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public final CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public final PointsFormat pointsFormat() {
        return new Lucene60PointsFormat();
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    private final NormsFormat normsFormat = new Lucene80NormsFormat();

    @Override
    public final NormsFormat normsFormat() {
        return normsFormat;
    }

    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        return KnnVectorsFormat.EMPTY;
    }
}
