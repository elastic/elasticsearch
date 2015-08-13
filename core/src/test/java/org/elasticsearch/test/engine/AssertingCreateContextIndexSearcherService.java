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

package org.elasticsearch.test.engine;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Random;
import java.util.Set;

public class AssertingCreateContextIndexSearcherService extends DefaultCreateContextIndexSearcherService {

    private final Random random;

    @Inject
    public AssertingCreateContextIndexSearcherService(@IndexSettings Settings indexSettings, Set<IndexSearcherWrapper> wrappers) {
        super(wrappers);
        final long seed = indexSettings.getAsLong(ESIntegTestCase.SETTING_INDEX_SEED, 0l);
        this.random = new Random(seed);
    }

    @Override
    protected Engine.Searcher wrapEngineSearcher(EngineConfig engineConfig, final ContextIndexSearcher indexSearcher, final Engine.Searcher originalEngineSearcher) {
        final Engine.Searcher newEngineSearcher = super.wrapEngineSearcher(engineConfig, indexSearcher, originalEngineSearcher);
        AssertingContextIndexSearcher assertingIndexSearcher = new AssertingContextIndexSearcher(random, indexSearcher);
        return new Engine.Searcher(newEngineSearcher.source(), assertingIndexSearcher) {

            @Override
            public void close() throws ElasticsearchException {
                newEngineSearcher.close();
            }
        };
    }
}
