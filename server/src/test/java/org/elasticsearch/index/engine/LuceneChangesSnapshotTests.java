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

package org.elasticsearch.index.engine;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class LuceneChangesSnapshotTests extends EngineTestCase {

    public void testEmptyEngine() throws Exception {
        MapperService mapper = createMapperService("test");
        long fromSeqNo = randomNonNegativeLong();
        long toSeqNo = randomLongBetween(fromSeqNo, Long.MAX_VALUE);
        try (Translog.Snapshot snapshot = engine.newLuceneChangesSnapshot("test", mapper, fromSeqNo, toSeqNo, true)) {
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
            assertThat(error.getMessage(),
                containsString("not all operations between min_seqno [" + fromSeqNo + "] and max_seqno [" + toSeqNo + "] found"));
        }
        try (Translog.Snapshot snapshot = engine.newLuceneChangesSnapshot("test", mapper, fromSeqNo, toSeqNo, false)) {
            assertThat(drainAll(snapshot), empty());
        }
    }

    public void testRequiredFullRange() throws Exception {
        MapperService mapper = createMapperService("test");
        int numOps = between(0, 100);
        for (int i = 0; i < numOps; i++) {
            String id = Integer.toString(randomIntBetween(i, i + 5));
            ParsedDocument doc = createParsedDoc(id, null);
            if (randomBoolean()) {
                engine.index(indexForDoc(doc));
            } else {
                engine.delete(new Engine.Delete(doc.type(), doc.id(), newUid(doc.id()), primaryTerm.get()));
            }
            if (rarely()) {
                engine.flush();
            }
        }
        int iters = between(1, 10);
        for (int i = 0; i < iters; i++) {

        }
    }

    public void testDedupByPrimaryTerm() throws Exception {
        MapperService mapper = createMapperService("test");
        Map<Long, Long> latestOperations = new HashMap<>();
        int numOps = scaledRandomIntBetween(100, 2000);
        List<Long> seqNos = LongStream.range(0, numOps).boxed().collect(Collectors.toList());
        Randomness.shuffle(seqNos);
        for (int i = 0; i < numOps; i++) {
            if (randomBoolean()) {
                primaryTerm.set(randomLongBetween(primaryTerm.get(), Long.MAX_VALUE));
                engine.rollTranslogGeneration();
            }
            if (randomBoolean()) {
                primaryTerm.set(randomLongBetween(1, primaryTerm.get()));
            }
            String id = Integer.toString(randomIntBetween(i, i + 5));
            ParsedDocument doc = createParsedDoc(id, null);
            final long seqNo = seqNos.remove(0);
            if (randomBoolean()) {
                engine.index(replicaIndexForDoc(doc, randomNonNegativeLong(), seqNo, false));
            } else {
                engine.delete(replicaDeleteForDoc(doc.id(), randomNonNegativeLong(), seqNo, threadPool.relativeTimeInMillis()));
            }
            latestOperations.put(seqNo, primaryTerm.get());
            if (rarely()) {
                engine.flush();
            }
        }
        final boolean requiredFullRange = randomBoolean();
        long fromSeqNo = randomLongBetween(0, numOps);
        long toSeqNo = randomLongBetween(fromSeqNo, requiredFullRange ? numOps : numOps * 2);
        try (Translog.Snapshot snapshot = engine.newLuceneChangesSnapshot("test", mapper, fromSeqNo, toSeqNo, requiredFullRange)) {
            List<Translog.Operation> ops = drainAll(snapshot);
            for (Translog.Operation op : ops) {
                assertThat(op.toString(), op.primaryTerm(), equalTo(latestOperations.get(op.seqNo())));
            }
        }
    }

    public void testOutOfOrderOperations() throws Exception {

    }

    public void testUpdateAndReplicateOperationsConcurrently() throws Exception {
        //
    }

    List<Translog.Operation> drainAll(Translog.Snapshot snapshot) throws IOException {
        List<Translog.Operation> operations = new ArrayList<>();
        Translog.Operation op;
        while ((op = snapshot.next()) != null) {
            operations.add(op);
        }
        return operations;
    }
}
