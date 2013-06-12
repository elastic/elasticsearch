/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import org.apache.lucene.index.*;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.Directory;
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
import java.util.List;
import java.util.Map;

/**
 * A {@link MergePolicy} that upgrades segments.
 * <p>
 * It can be useful to use the background merging process to upgrade segments,
 * for example when we perform internal changes that imply different index
 * options or when a user modifies his mapping in non-breaking ways: we could
 * imagine using this merge policy to be able to add doc values to fields after
 * the fact or on the opposite to remove them.
 * <p>
 * For now, this {@link MergePolicy} takes care of moving versions that used to
 * be stored as payloads to numeric doc values.
 */
public final class IndexUpgraderMergePolicy extends MergePolicy {

    private final MergePolicy delegate;

    /** @param delegate the merge policy to wrap */
    public IndexUpgraderMergePolicy(MergePolicy delegate) {
        this.delegate = delegate;
    }

    /** Return an "upgraded" view of the reader. */
    static AtomicReader filter(AtomicReader reader) throws IOException {
        final FieldInfos fieldInfos = reader.getFieldInfos();
        final FieldInfo versionInfo = fieldInfos.fieldInfo(VersionFieldMapper.NAME);
        if (versionInfo != null && versionInfo.hasDocValues()) {
            // the reader is a recent one, it has versions and they are stored
            // in a numeric doc values field
            return reader;
        }
        // The segment is an old one, load all versions in memory and hide
        // them behind a numeric doc values field
        final Terms terms = reader.terms(UidFieldMapper.NAME);
        if (terms == null || !terms.hasPayloads()) {
            // The segment doesn't have an _uid field or doesn't have paylods
            // don't try to do anything clever. If any other segment has versions
            // all versions of this segment will be initialized to 0
            return reader;
        }
        final TermsEnum uids = terms.iterator(null);
        final GrowableWriter versions = new GrowableWriter(2, reader.maxDoc(), PackedInts.DEFAULT);
        DocsAndPositionsEnum dpe = null;
        for (BytesRef uid = uids.next(); uid != null; uid = uids.next()) {
            dpe = uids.docsAndPositions(reader.getLiveDocs(), dpe, DocsAndPositionsEnum.FLAG_PAYLOADS);
            assert dpe != null : "field has payloads";
            for (int doc = dpe.nextDoc(); doc != DocsEnum.NO_MORE_DOCS; doc = dpe.nextDoc()) {
                dpe.nextPosition();
                final BytesRef payload = dpe.getPayload();
                if (payload != null && payload.length == 8) {
                    final long version = Numbers.bytesToLong(payload);
                    versions.set(doc, version);
                    break;
                }
            }
        }
        // Build new field infos, doc values, and return a filter reader
        final FieldInfo newVersionInfo;
        if (versionInfo == null) {
            // Find a free field number
            int fieldNumber = 0;
            for (FieldInfo fi : fieldInfos) {
                fieldNumber = Math.max(fieldNumber, fi.number + 1);
            }
            newVersionInfo = new FieldInfo(VersionFieldMapper.NAME, false, fieldNumber, false, true, false,
                    IndexOptions.DOCS_ONLY, DocValuesType.NUMERIC, DocValuesType.NUMERIC, Collections.<String, String>emptyMap());
        } else {
            newVersionInfo = new FieldInfo(VersionFieldMapper.NAME, versionInfo.isIndexed(), versionInfo.number,
                    versionInfo.hasVectors(), versionInfo.omitsNorms(), versionInfo.hasPayloads(),
                    versionInfo.getIndexOptions(), versionInfo.getDocValuesType(), versionInfo.getNormType(), versionInfo.attributes());
        }
        final ArrayList<FieldInfo> fieldInfoList = new ArrayList<FieldInfo>();
        for (FieldInfo info : fieldInfos) {
            if (info != versionInfo) {
                fieldInfoList.add(info);
            }
        }
        fieldInfoList.add(newVersionInfo);
        final FieldInfos newFieldInfos = new FieldInfos(fieldInfoList.toArray(new FieldInfo[fieldInfoList.size()]));
        final NumericDocValues versionValues = new NumericDocValues() {
            @Override
            public long get(int index) {
                return versions.get(index);
            }
        };
        return new FilterAtomicReader(reader) {
            @Override
            public FieldInfos getFieldInfos() {
                return newFieldInfos;
            }
            @Override
            public NumericDocValues getNumericDocValues(String field) throws IOException {
                if (VersionFieldMapper.NAME.equals(field)) {
                    return versionValues;
                }
                return super.getNumericDocValues(field);
            }
            @Override
            public Bits getDocsWithField(String field) throws IOException {
                return new Bits.MatchAllBits(in.maxDoc());
            }
        };
    }

    static class IndexUpgraderOneMerge extends OneMerge {

        public IndexUpgraderOneMerge(List<SegmentInfoPerCommit> segments) {
            super(segments);
        }

        @Override
        public List<AtomicReader> getMergeReaders() throws IOException {
            final List<AtomicReader> readers = super.getMergeReaders();
            ImmutableList.Builder<AtomicReader> newReaders = ImmutableList.builder();
            for (AtomicReader reader : readers) {
                newReaders.add(filter(reader));
            }
            return newReaders.build();
        }

    }

    static class IndexUpgraderMergeSpecification extends MergeSpecification {

        @Override
        public void add(OneMerge merge) {
          super.add(new IndexUpgraderOneMerge(merge.segments));
        }

        @Override
        public String segString(Directory dir) {
            return "IndexUpgraderMergeSpec[" + super.segString(dir) + "]";
        }

    }

    static MergeSpecification upgradedMergeSpecification(MergeSpecification spec) {
        if (spec == null) {
            return null;
        }
        MergeSpecification upgradedSpec = new IndexUpgraderMergeSpecification();
        for (OneMerge merge : spec.merges) {
          upgradedSpec.add(merge);
        }
        return upgradedSpec;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger,
        SegmentInfos segmentInfos) throws IOException {
      return upgradedMergeSpecification(delegate.findMerges(mergeTrigger, segmentInfos));
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos segmentInfos,
        int maxSegmentCount, Map<SegmentInfoPerCommit,Boolean> segmentsToMerge)
        throws IOException {
      return upgradedMergeSpecification(delegate.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge));
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos)
        throws IOException {
      return upgradedMergeSpecification(delegate.findForcedDeletesMerges(segmentInfos));
    }

    @Override
    public MergePolicy clone() {
      return new IndexUpgraderMergePolicy(delegate.clone());
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    public boolean useCompoundFile(SegmentInfos segments,
        SegmentInfoPerCommit newSegment) throws IOException {
      return delegate.useCompoundFile(segments, newSegment);
    }

    @Override
    public void setIndexWriter(IndexWriter writer) {
      delegate.setIndexWriter(writer);
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + delegate + ")";
    }

}
