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
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene60;

import org.apache.lucene.backward_codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.elasticsearch.xpack.lucene.bwc.codecs.BWCCodec;
import org.elasticsearch.xpack.lucene.bwc.codecs.LegacyAdaptingPerFieldPostingsFormat;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene50.Lucene50SegmentInfoFormat;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene54.Lucene54DocValuesFormat;

/**
 * Implements the Lucene 6.0 index format.
 *
 * @deprecated Only for 6.0 back compat
 */
@Deprecated
public class Lucene60Codec extends BWCCodec {

    private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();
    private final StoredFieldsFormat storedFieldsFormat;
    private final DocValuesFormat defaultDocValuesFormat = new Lucene54DocValuesFormat();
    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return defaultDocValuesFormat;
        }
    };
    private final PostingsFormat postingsFormat = new LegacyAdaptingPerFieldPostingsFormat();

    /**
     * Instantiates a new codec. Called by SPI.
     */
    @SuppressWarnings("unused")
    public Lucene60Codec() {
        super("Lucene60");
        this.storedFieldsFormat = new Lucene50StoredFieldsFormat(Lucene50StoredFieldsFormat.Mode.BEST_SPEED);
    }

    @Override
    protected FieldInfosFormat originalFieldInfosFormat() {
        return new Lucene60FieldInfosFormat();
    }

    @Override
    protected SegmentInfoFormat originalSegmentInfoFormat() {
        return new Lucene50SegmentInfoFormat();
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
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
    public DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public PointsFormat pointsFormat() {
        return new Lucene60MetadataOnlyPointsFormat();
    }
}
