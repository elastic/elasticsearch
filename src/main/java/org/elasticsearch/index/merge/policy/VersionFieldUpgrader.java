/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.merge.policy;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Converts 0.90.x _uid payloads to _version docvalues
 */
class VersionFieldUpgrader extends FilterCodecReader {
    final FieldInfos infos;
    
    VersionFieldUpgrader(CodecReader in) {
        super(in);

        // Find a free field number
        int fieldNumber = 0;
        for (FieldInfo fi : in.getFieldInfos()) {
            fieldNumber = Math.max(fieldNumber, fi.number + 1);
        }
            
        // TODO: lots of things can wrong here...
        FieldInfo newInfo = new FieldInfo(VersionFieldMapper.NAME,               // field name
                                          fieldNumber,                           // field number
                                          false,                                 // store term vectors
                                          false,                                 // omit norms
                                          false,                                 // store payloads
                                          IndexOptions.NONE,                     // index options
                                          DocValuesType.NUMERIC,                 // docvalues
                                          -1,                                    // docvalues generation
                                          Collections.<String, String>emptyMap() // attributes
                                          );
        newInfo.checkConsistency(); // fail merge immediately if above code is wrong
        
        final ArrayList<FieldInfo> fieldInfoList = new ArrayList<>();
        for (FieldInfo info : in.getFieldInfos()) {
            if (!info.name.equals(VersionFieldMapper.NAME)) {
                fieldInfoList.add(info);
            }
        }
        fieldInfoList.add(newInfo);
        infos = new FieldInfos(fieldInfoList.toArray(new FieldInfo[fieldInfoList.size()]));
    }
    
    static CodecReader wrap(CodecReader reader) throws IOException {
        final FieldInfos fieldInfos = reader.getFieldInfos();
        final FieldInfo versionInfo = fieldInfos.fieldInfo(VersionFieldMapper.NAME);
        if (versionInfo != null && versionInfo.getDocValuesType() != DocValuesType.NONE) {
            // the reader is a recent one, it has versions and they are stored
            // in a numeric doc values field
            return reader;
        }
        // The segment is an old one, look at the _uid field
        final Terms terms = reader.terms(UidFieldMapper.NAME);
        if (terms == null || !terms.hasPayloads()) {
            // The segment doesn't have an _uid field or doesn't have payloads
            // don't try to do anything clever. If any other segment has versions
            // all versions of this segment will be initialized to 0
            return reader;
        }
        // convert _uid payloads -> _version docvalues
        return new VersionFieldUpgrader(reader);
    }

    @Override
    public FieldInfos getFieldInfos() {
        return infos;
    }

    @Override
    public DocValuesProducer getDocValuesReader() {
        DocValuesProducer producer = in.getDocValuesReader();
        // TODO: move this nullness stuff out
        if (producer == null) {
            producer = FilterDocValuesProducer.EMPTY;
        }
        return new UninvertedVersions(producer, this);
    }
    
    static class UninvertedVersions extends FilterDocValuesProducer {
        final CodecReader reader;
        
        UninvertedVersions(DocValuesProducer in, CodecReader reader) {
            super(in);
            this.reader = reader;
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            if (VersionFieldMapper.NAME.equals(field.name)) {
                // uninvert into a packed ints and expose as docvalues
                final Terms terms = reader.terms(UidFieldMapper.NAME);
                final TermsEnum uids = terms.iterator();
                final GrowableWriter versions = new GrowableWriter(2, reader.maxDoc(), PackedInts.COMPACT);
                PostingsEnum dpe = null;
                for (BytesRef uid = uids.next(); uid != null; uid = uids.next()) {
                    dpe = uids.postings(reader.getLiveDocs(), dpe, PostingsEnum.PAYLOADS);
                    assert terms.hasPayloads() : "field has payloads";
                    for (int doc = dpe.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dpe.nextDoc()) {
                        dpe.nextPosition();
                        final BytesRef payload = dpe.getPayload();
                        if (payload != null && payload.length == 8) {
                            final long version = Numbers.bytesToLong(payload);
                            versions.set(doc, version);
                            break;
                        }
                    }
                }
                return versions;
            } else {
                return in.getNumeric(field);
            }
        }

        @Override
        public Bits getDocsWithField(FieldInfo field) throws IOException {
            if (VersionFieldMapper.NAME.equals(field.name)) {
                return new Bits.MatchAllBits(reader.maxDoc());
            } else {
                return in.getDocsWithField(field);
            }
        }

        @Override
        public DocValuesProducer getMergeInstance() throws IOException {
            return new UninvertedVersions(in.getMergeInstance(), reader);
        }
    }
}
