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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class NoopEngineTests extends EngineTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);

    public void testNoopEngine() throws IOException {
        engine.close();
        final NoopEngine engine = new NoopEngine(noopConfig(INDEX_SETTINGS, store, primaryTranslogDir));
        expectThrows(UnsupportedOperationException.class, () -> engine.index(null));
        expectThrows(UnsupportedOperationException.class, () -> engine.delete(null));
        expectThrows(UnsupportedOperationException.class, () -> engine.noOp(null));
        expectThrows(UnsupportedOperationException.class, () -> engine.syncFlush(null, null));
        expectThrows(UnsupportedOperationException.class, () -> engine.get(null, null));
        expectThrows(UnsupportedOperationException.class, () -> engine.acquireSearcher(null, null));
        expectThrows(UnsupportedOperationException.class, () -> engine.ensureTranslogSynced(null));
        expectThrows(UnsupportedOperationException.class, engine::activateThrottling);
        expectThrows(UnsupportedOperationException.class, engine::deactivateThrottling);
        assertThat(engine.refreshNeeded(), equalTo(false));
        assertThat(engine.shouldPeriodicallyFlush(), equalTo(false));
        engine.close();
    }

    public void testTwoNoopEngines() throws IOException {
        engine.close();
        // It's so noop you can even open two engines for the same store without tripping anything
        final NoopEngine engine1 = new NoopEngine(noopConfig(INDEX_SETTINGS, store, primaryTranslogDir));
        final NoopEngine engine2 = new NoopEngine(noopConfig(INDEX_SETTINGS, store, primaryTranslogDir));
        engine1.close();
        engine2.close();
    }

}
