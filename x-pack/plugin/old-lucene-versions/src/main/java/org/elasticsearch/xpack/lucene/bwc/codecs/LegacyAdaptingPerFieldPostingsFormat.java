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
package org.elasticsearch.xpack.lucene.bwc.codecs;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene50.BWCLucene50PostingsFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Modified version of {@link PerFieldPostingsFormat} that allows swapping in
 * {@link org.elasticsearch.xpack.lucene.bwc.codecs.lucene50.BWCLucene50PostingsFormat} instead of
 * {@link org.apache.lucene.backward_codecs.lucene50.Lucene50PostingsFormat} when reading from older
 * codecs. The former has full support for older Lucene versions (going back to Lucene 5) while the
 * latter only supports Lucene 7 and above (as it was shipped with backwards-codecs of Lucene 9 that
 * only has support for N-2).
 *
 * This class can be removed once Elasticsearch gets upgraded to Lucene 11 and Lucene50PostingsFormat is no longer
 * shipped as part of bwc jars.
 */
@UpdateForV10(owner = UpdateForV10.Owner.SEARCH_FOUNDATIONS)
public final class LegacyAdaptingPerFieldPostingsFormat extends PostingsFormat {
    /** Name of this {@link PostingsFormat}. */
    public static final String PER_FIELD_NAME = "PerField40";

    /** {@link FieldInfo} attribute name used to store the format name for each field. */
    public static final String PER_FIELD_FORMAT_KEY = PerFieldPostingsFormat.class.getSimpleName() + ".format";

    /** {@link FieldInfo} attribute name used to store the segment suffix name for each field. */
    public static final String PER_FIELD_SUFFIX_KEY = PerFieldPostingsFormat.class.getSimpleName() + ".suffix";

    /** Sole constructor. */
    public LegacyAdaptingPerFieldPostingsFormat() {
        super(PER_FIELD_NAME);
    }

    static String getSuffix(String formatName, String suffix) {
        return formatName + "_" + suffix;
    }

    private static PostingsFormat getPostingsFormat(String formatName) {
        if (formatName.equals("Lucene50")) {
            return new BWCLucene50PostingsFormat();
        } else {
            return new BWCCodec.EmptyPostingsFormat();
        }
    }

    private static class FieldsReader extends FieldsProducer {

        private final Map<String, FieldsProducer> fields = new TreeMap<>();
        private final Map<String, FieldsProducer> formats = new HashMap<>();
        private final String segment;

        // clone for merge
        FieldsReader(FieldsReader other) {
            Map<FieldsProducer, FieldsProducer> oldToNew = new IdentityHashMap<>();
            // First clone all formats
            for (Map.Entry<String, FieldsProducer> ent : other.formats.entrySet()) {
                FieldsProducer values = ent.getValue().getMergeInstance();
                formats.put(ent.getKey(), values);
                oldToNew.put(ent.getValue(), values);
            }

            // Then rebuild fields:
            for (Map.Entry<String, FieldsProducer> ent : other.fields.entrySet()) {
                FieldsProducer producer = oldToNew.get(ent.getValue());
                assert producer != null;
                fields.put(ent.getKey(), producer);
            }

            segment = other.segment;
        }

        FieldsReader(final SegmentReadState readState) throws IOException {

            // Read _X.per and init each format:
            boolean success = false;
            try {
                // Read field name -> format name
                for (FieldInfo fi : readState.fieldInfos) {
                    if (fi.getIndexOptions() != IndexOptions.NONE) {
                        final String fieldName = fi.name;
                        final String formatName = fi.getAttribute(PER_FIELD_FORMAT_KEY);
                        if (formatName != null) {
                            // null formatName means the field is in fieldInfos, but has no postings!
                            final String suffix = fi.getAttribute(PER_FIELD_SUFFIX_KEY);
                            if (suffix == null) {
                                throw new IllegalStateException("missing attribute: " + PER_FIELD_SUFFIX_KEY + " for field: " + fieldName);
                            }
                            PostingsFormat format = getPostingsFormat(formatName);
                            String segmentSuffix = getSuffix(formatName, suffix);
                            if (formats.containsKey(segmentSuffix) == false) {
                                formats.put(segmentSuffix, format.fieldsProducer(new SegmentReadState(readState, segmentSuffix)));
                            }
                            fields.put(fieldName, formats.get(segmentSuffix));
                        }
                    }
                }
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(formats.values());
                }
            }

            this.segment = readState.segmentInfo.name;
        }

        @Override
        public Iterator<String> iterator() {
            return Collections.unmodifiableSet(fields.keySet()).iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            FieldsProducer fieldsProducer = fields.get(field);
            return fieldsProducer == null ? null : fieldsProducer.terms(field);
        }

        @Override
        public int size() {
            return fields.size();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(formats.values());
        }

        @Override
        public void checkIntegrity() throws IOException {
            for (FieldsProducer producer : formats.values()) {
                producer.checkIntegrity();
            }
        }

        @Override
        public FieldsProducer getMergeInstance() {
            return new FieldsReader(this);
        }

        @Override
        public String toString() {
            return "PerFieldPostings(segment=" + segment + " formats=" + formats.size() + ")";
        }
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) {
        throw new IllegalStateException("This codec should only be used for reading, not writing");
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FieldsReader(state);
    }
}
