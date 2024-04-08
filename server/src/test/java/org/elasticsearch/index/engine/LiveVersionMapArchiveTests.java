/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;

import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.putIndex;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.randomIndexVersionValue;

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
}
