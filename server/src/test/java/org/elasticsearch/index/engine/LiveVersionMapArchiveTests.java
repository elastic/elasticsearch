/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.putIndex;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.randomIndexVersionValue;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.uid;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LiveVersionMapArchiveTests extends IndexShardTestCase {
    private class TestArchive implements LiveVersionMapArchive {

        boolean afterRefreshCalled = false;
        LiveVersionMap.VersionLookup archivedMap = null;

        @Override
        public void afterRefresh(LiveVersionMap.VersionLookup old) {
            afterRefreshCalled = true;
            archivedMap = old;
        }

        @Override
        public VersionValue get(BytesRef uid) {
            return null;
        }

        @Override
        public long getMinDeleteTimestamp() {
            return Long.MAX_VALUE;
        }
    }

    private class TestEngine extends InternalEngine {
        TestEngine(EngineConfig engineConfig) {
            super(engineConfig);
        }

        @Override
        public LiveVersionMapArchive createLiveVersionMapArchive() {
            return new TestArchive();
        }
    }

    public void testLiveVersionMapArchiveCreation() throws Exception {
        final IndexShard shard = newStartedShard(false, Settings.EMPTY, TestEngine::new);
        assertThat(shard.getEngineOrNull(), CoreMatchers.instanceOf(InternalEngine.class));
        var engine = (InternalEngine) shard.getEngineOrNull();
        assertThat(engine.getLiveVersionMapArchive(), IsInstanceOf.instanceOf(TestArchive.class));
        closeShards(shard);
    }

    public void testLiveVersionMapArchiveBasic() throws Exception {
        TestArchive archive = new TestArchive();
        LiveVersionMap map = new LiveVersionMap(archive);
        putIndex(map, "1", randomIndexVersionValue());
        map.beforeRefresh();
        assertFalse(archive.afterRefreshCalled);
        assertNull(archive.archivedMap);
        map.afterRefresh(randomBoolean());
        assertTrue(archive.afterRefreshCalled);
        var archived = archive.archivedMap;
        assertNotNull(archived);
        assertEquals(archived.size(), 1);
    }

    /** An archive that returns whatever the test puts in {@link #entries}, to stand in for an archive holding a stale entry. */
    private static class FixedArchive implements LiveVersionMapArchive {
        final Map<BytesRef, VersionValue> entries = new ConcurrentHashMap<>();

        @Override
        public void afterRefresh(LiveVersionMap.VersionLookup old) {}

        @Override
        public VersionValue get(BytesRef uid) {
            return entries.get(uid);
        }

        @Override
        public long getMinDeleteTimestamp() {
            return Long.MAX_VALUE;
        }
    }

    private IndexShard newStartedShard(LiveVersionMapArchive archive) throws IOException {
        return newStartedShard(true, Settings.EMPTY, config -> new InternalEngine(config) {
            @Override
            protected LiveVersionMapArchive createLiveVersionMapArchive() {
                return archive;
            }
        });
    }

    private static Engine.IndexResult index(IndexShard shard, String id, String source, long version, long ifSeqNo, long ifPrimaryTerm)
        throws IOException {
        return shard.applyIndexOperationOnPrimary(
            version,
            VersionType.INTERNAL,
            new SourceToParse(id, new BytesArray(source), XContentType.JSON, null),
            ifSeqNo,
            ifPrimaryTerm,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false
        );
    }

    private static String getForUpdateSource(IndexShard shard, String id, long ifSeqNo, long ifPrimaryTerm) throws IOException {
        final Engine.Get get = new Engine.Get(true, true, id).setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm);
        try (
            Engine.GetResult result = shard.getEngineOrNull()
                .getForUpdate(get, shard.mapperService().mappingLookup(), shard.mapperService().documentParser(), Function.identity())
        ) {
            assertTrue(result.exists());
            final FieldsVisitor visitor = new FieldsVisitor(true);
            result.docIdAndVersion().reader.storedFields().document(result.docIdAndVersion().docId, visitor);
            return visitor.source().utf8ToString();
        }
    }

    public void testWritesIgnoreStaleArchiveEntry() throws Exception {
        final FixedArchive archive = new FixedArchive();
        final IndexShard shard = newStartedShard(archive);
        try {
            final long term = shard.getOperationPrimaryTerm();
            final Engine.IndexResult first = indexDoc(shard, "_doc", "1", "{\"f\":\"v1\"}");
            final Engine.IndexResult second = indexDoc(shard, "_doc", "1", "{\"f\":\"v2\"}");
            // the refresh moves the doc out of the live maps, so lookups fall through to the archive and the internal reader
            shard.refresh("test");
            // Poison the archive with a wrong entry. Due to a bug in LVM, it could skip a put.
            // See https://github.com/elastic/elasticsearch/issues/149677
            final Translog.OperationLocation firstLocation = new Translog.OperationLocation(first.getTranslogLocation());
            archive.entries.put(uid("1"), new IndexVersionValue(firstLocation, first.getVersion(), first.getSeqNo(), term));

            assertThat(getForUpdateSource(shard, "1", UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM), equalTo("{\"f\":\"v2\"}"));
            assertThat(getForUpdateSource(shard, "1", second.getSeqNo(), term), equalTo("{\"f\":\"v2\"}"));
            final Engine.IndexResult stale = index(shard, "1", "{\"f\":\"v3\"}", Versions.MATCH_ANY, first.getSeqNo(), term);
            assertThat(stale.getFailure(), instanceOf(VersionConflictEngineException.class));
            final Engine.IndexResult current = index(shard, "1", "{\"f\":\"v3\"}", Versions.MATCH_ANY, second.getSeqNo(), term);
            assertThat(current.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
        } finally {
            closeShards(shard);
        }
    }
}
