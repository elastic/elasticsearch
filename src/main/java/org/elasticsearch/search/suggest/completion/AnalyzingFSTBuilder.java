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

package org.elasticsearch.search.suggest.completion;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.*;
import org.elasticsearch.common.collect.HppcMaps;

import java.io.IOException;

public class AnalyzingFSTBuilder {
    private Builder<PairOutputs.Pair<Long, BytesRef>> builder;
    private int maxSurfaceFormsPerAnalyzedForm;
    private IntsRef scratchInts = new IntsRef();
    private final PairOutputs<Long, BytesRef> outputs;
    private boolean hasPayloads;
    private BytesRef analyzed = new BytesRef();
    private final SurfaceFormAndPayload[] surfaceFormsAndPayload;
    private int count;
    private ObjectIntOpenHashMap<BytesRef> seenSurfaceForms = HppcMaps.Object.Integer.ensureNoNullKeys(256, 0.75f);
    private int payloadSep;
    private int docID;

    public AnalyzingFSTBuilder(int maxSurfaceFormsPerAnalyzedForm, boolean hasPayloads, int payloadSep) {
        this.payloadSep = payloadSep;
        this.outputs = new PairOutputs<>(PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton());
        this.builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
        this.maxSurfaceFormsPerAnalyzedForm = maxSurfaceFormsPerAnalyzedForm;
        this.hasPayloads = hasPayloads;
        surfaceFormsAndPayload = new SurfaceFormAndPayload[maxSurfaceFormsPerAnalyzedForm];

    }

    public void startTerm(BytesRef analyzed) {
        this.analyzed.copyBytes(analyzed);
        this.analyzed.grow(analyzed.length + 2);
    }

    public void setDocID(final int docID) {
        this.docID = docID;
    }

    private final static class SurfaceFormAndPayload implements Comparable<SurfaceFormAndPayload> {
        int docID;
        BytesRef payload;
        long weight;

        public SurfaceFormAndPayload(int docID, BytesRef payload, long cost) {
            super();
            this.docID = docID;
            this.payload = payload;
            this.weight = cost;
        }

        @Override
        public int compareTo(SurfaceFormAndPayload o) {
            int res = compare((long) docID, (long) o.docID);
            if (res == 0) {
                res = compare(weight, o.weight);
                if (res == 0) {
                    return payload.compareTo(o.payload);
                }
            }
            return res;
        }

        /**
         *  returns payload + SEP + docID
         */
        public BytesRef payloadWithDocID(int sep) throws IOException {
            int len = payload.length - payload.offset + 1 + 4;
            byte[] buffer = new byte[len];
            ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);
            output.writeBytes(payload.bytes, 0, payload.length - payload.offset);
            output.writeByte((byte) sep);
            output.writeInt(docID);
            return new BytesRef(buffer, 0, len);
        }

        public static int compare(long x, long y) {
            return (x < y) ? -1 : ((x == y) ? 0 : 1);
        }
    }

    public void addSurface(BytesRef surface, BytesRef payload, long cost) throws IOException {
        int surfaceIndex = -1;
        long encodedWeight = cost == -1 ? cost : XAnalyzingSuggester.encodeWeight(cost);
            /*
             * we need to check if we have seen this surface form, if so only use the
             * the surface form with the highest weight and drop the rest no matter if
             * the payload differs.
             */
        if (count >= maxSurfaceFormsPerAnalyzedForm) {
            // More than maxSurfaceFormsPerAnalyzedForm
            // dups: skip the rest:
            return;
        }
        BytesRef surfaceCopy;
        if (count > 0 && seenSurfaceForms.containsKey(surface)) {
            surfaceIndex = seenSurfaceForms.lget();
            SurfaceFormAndPayload surfaceFormAndPayload = surfaceFormsAndPayload[surfaceIndex];
            if (encodedWeight >= surfaceFormAndPayload.weight) {
                return;
            }
            surfaceCopy = BytesRef.deepCopyOf(surface);
        } else {
            surfaceIndex = count++;
            surfaceCopy = BytesRef.deepCopyOf(surface);
            seenSurfaceForms.put(surfaceCopy, surfaceIndex);
        }

        BytesRef payloadRef;
        if (!hasPayloads) {
            payloadRef = surfaceCopy;
        } else {
            int len = surface.length + 1 + payload.length;
            final BytesRef br = new BytesRef(len);
            System.arraycopy(surface.bytes, surface.offset, br.bytes, 0, surface.length);
            br.bytes[surface.length] = (byte) payloadSep;
            System.arraycopy(payload.bytes, payload.offset, br.bytes, surface.length + 1, payload.length);
            br.length = len;
            payloadRef = br;
        }
        if (surfaceFormsAndPayload[surfaceIndex] == null) {
            surfaceFormsAndPayload[surfaceIndex] = new SurfaceFormAndPayload(this.docID, payloadRef, encodedWeight);
        } else {
            surfaceFormsAndPayload[surfaceIndex].payload = payloadRef;
            surfaceFormsAndPayload[surfaceIndex].weight = encodedWeight;
            surfaceFormsAndPayload[surfaceIndex].docID = this.docID;
        }
    }

    public void finishTerm(long defaultWeight) throws IOException {
        ArrayUtil.timSort(surfaceFormsAndPayload, 0, count);
        int deduplicator = 0;
        analyzed.bytes[analyzed.offset + analyzed.length] = 0;
        analyzed.length += 2;
        for (int i = 0; i < count; i++) {
            analyzed.bytes[analyzed.offset + analyzed.length - 1] = (byte) deduplicator++;
            Util.toIntsRef(analyzed, scratchInts);
            SurfaceFormAndPayload candidate = surfaceFormsAndPayload[i];
            long cost = candidate.weight == -1 ? XAnalyzingSuggester.encodeWeight(Math.min(Integer.MAX_VALUE, defaultWeight)) : candidate.weight;
            builder.add(scratchInts, outputs.newPair(cost, candidate.payloadWithDocID(payloadSep)));
        }
        seenSurfaceForms.clear();
        count = 0;
    }

    public FST<PairOutputs.Pair<Long, BytesRef>> build() throws IOException {
        return builder.finish();
    }

    public boolean hasPayloads() {
        return hasPayloads;
    }

    public int maxSurfaceFormsPerAnalyzedForm() {
        return maxSurfaceFormsPerAnalyzedForm;
    }

}
