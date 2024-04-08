/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.basic;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.tests.util.English;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.engine.ThrowingLeafReaderWrapper;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class SearchWithRandomExceptionsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(RandomExceptionDirectoryReaderWrapper.TestPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testRandomExceptions() throws IOException, InterruptedException, ExecutionException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("test")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
        );
        final double lowLevelRate;
        final double topLevelRate;
        if (frequently()) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    lowLevelRate = 1.0 / between(2, 10);
                    topLevelRate = 0.0d;
                } else {
                    topLevelRate = 1.0 / between(2, 10);
                    lowLevelRate = 0.0d;
                }
            } else {
                lowLevelRate = 1.0 / between(2, 10);
                topLevelRate = 1.0 / between(2, 10);
            }
        } else {
            // rarely no exception
            topLevelRate = 0d;
            lowLevelRate = 0d;
        }

        Builder settings = Settings.builder()
            .put(indexSettings())
            .put(EXCEPTION_TOP_LEVEL_RATIO_KEY, topLevelRate)
            .put(EXCEPTION_LOW_LEVEL_RATIO_KEY, lowLevelRate)
            .put(MockEngineSupport.WRAP_READER_RATIO.getKey(), 1.0d);
        logger.info("creating index: [test] using settings: [{}]", settings.build());
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(mapping));
        ensureSearchable();
        final int numDocs = between(10, 100);
        int numCreated = 0;
        boolean[] added = new boolean[numDocs];
        for (int i = 0; i < numDocs; i++) {
            try {
                DocWriteResponse indexResponse = prepareIndex("test").setId("" + i)
                    .setTimeout(TimeValue.timeValueSeconds(1))
                    .setSource("test", English.intToEnglish(i))
                    .get();
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    numCreated++;
                    added[i] = true;
                }
            } catch (ElasticsearchException ex) {}
        }
        logger.info("Start Refresh");
        // don't assert on failures here
        BroadcastResponse refreshResponse = indicesAdmin().prepareRefresh("test").execute().get();
        final boolean refreshFailed = refreshResponse.getShardFailures().length != 0 || refreshResponse.getFailedShards() != 0;
        logger.info(
            "Refresh failed [{}] numShardsFailed: [{}], shardFailuresLength: [{}], successfulShards: [{}], totalShards: [{}] ",
            refreshFailed,
            refreshResponse.getFailedShards(),
            refreshResponse.getShardFailures().length,
            refreshResponse.getSuccessfulShards(),
            refreshResponse.getTotalShards()
        );

        NumShards test = getNumShards("test");
        final int numSearches = scaledRandomIntBetween(100, 200);
        final int finalNumCreated = numCreated;
        // we don't check anything here really just making sure we don't leave any open files or a broken index behind.
        for (int i = 0; i < numSearches; i++) {
            try {
                int docToQuery = between(0, numDocs - 1);
                int expectedResults = added[docToQuery] ? 1 : 0;
                logger.info("Searching for [test:{}]", English.intToEnglish(docToQuery));
                assertResponse(
                    prepareSearch().setQuery(QueryBuilders.matchQuery("test", English.intToEnglish(docToQuery))).setSize(expectedResults),
                    response -> {
                        logger.info("Successful shards: [{}]  numShards: [{}]", response.getSuccessfulShards(), test.numPrimaries);
                        if (response.getSuccessfulShards() == test.numPrimaries && refreshFailed == false) {
                            assertResultsAndLogOnFailure(expectedResults, response);
                        }
                    }
                );
                // check match all
                assertResponse(
                    prepareSearch().setQuery(QueryBuilders.matchAllQuery()).setSize(numCreated).addSort("_id", SortOrder.ASC),
                    response -> {
                        logger.info(
                            "Match all Successful shards: [{}]  numShards: [{}]",
                            response.getSuccessfulShards(),
                            test.numPrimaries
                        );
                        if (response.getSuccessfulShards() == test.numPrimaries && refreshFailed == false) {
                            assertResultsAndLogOnFailure(finalNumCreated, response);
                        }
                    }
                );

            } catch (SearchPhaseExecutionException ex) {
                logger.info("expected SearchPhaseException: [{}]", ex.getMessage());
            }
        }
    }

    public static final String EXCEPTION_TOP_LEVEL_RATIO_KEY = "index.engine.exception.ratio.top";
    public static final String EXCEPTION_LOW_LEVEL_RATIO_KEY = "index.engine.exception.ratio.low";

    public static class RandomExceptionDirectoryReaderWrapper extends MockEngineSupport.DirectoryReaderWrapper {

        public static class TestPlugin extends MockEngineFactoryPlugin {
            public static final Setting<Double> EXCEPTION_TOP_LEVEL_RATIO_SETTING = Setting.doubleSetting(
                EXCEPTION_TOP_LEVEL_RATIO_KEY,
                0.1d,
                0.0d,
                Property.IndexScope
            );
            public static final Setting<Double> EXCEPTION_LOW_LEVEL_RATIO_SETTING = Setting.doubleSetting(
                EXCEPTION_LOW_LEVEL_RATIO_KEY,
                0.1d,
                0.0d,
                Property.IndexScope
            );

            @Override
            public List<Setting<?>> getSettings() {
                List<Setting<?>> settings = new ArrayList<>();
                settings.addAll(super.getSettings());
                settings.add(EXCEPTION_TOP_LEVEL_RATIO_SETTING);
                settings.add(EXCEPTION_LOW_LEVEL_RATIO_SETTING);
                return settings;
            }

            @Override
            protected Class<? extends FilterDirectoryReader> getReaderWrapperClass() {
                return RandomExceptionDirectoryReaderWrapper.class;
            }
        }

        private final Settings settings;

        static class ThrowingSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper implements ThrowingLeafReaderWrapper.Thrower {
            private final Random random;
            private final double topLevelRatio;
            private final double lowLevelRatio;

            ThrowingSubReaderWrapper(Settings settings) {
                final long seed = ESIntegTestCase.INDEX_TEST_SEED_SETTING.get(settings);
                this.topLevelRatio = settings.getAsDouble(EXCEPTION_TOP_LEVEL_RATIO_KEY, 0.1d);
                this.lowLevelRatio = settings.getAsDouble(EXCEPTION_LOW_LEVEL_RATIO_KEY, 0.1d);
                this.random = new Random(seed);
            }

            @Override
            public LeafReader wrap(LeafReader reader) {
                return new ThrowingLeafReaderWrapper(reader, this);
            }

            @Override
            public void maybeThrow(ThrowingLeafReaderWrapper.Flags flag) throws IOException {
                switch (flag) {
                    case Fields:
                    case TermVectors:
                    case Terms:
                    case TermsEnum:
                    case Intersect:
                    case Norms:
                    case NumericDocValues:
                    case BinaryDocValues:
                    case SortedDocValues:
                    case SortedSetDocValues:
                        if (random.nextDouble() < topLevelRatio) {
                            throw new IOException("Forced top level Exception on [" + flag.name() + "]");
                        }
                        break;
                    case DocsEnum:
                    case DocsAndPositionsEnum:
                        if (random.nextDouble() < lowLevelRatio) {
                            throw new IOException("Forced low level Exception on [" + flag.name() + "]");
                        }
                        break;
                }
            }

            @Override
            public boolean wrapTerms(String field) {
                return true;
            }
        }

        public RandomExceptionDirectoryReaderWrapper(DirectoryReader in, Settings settings) throws IOException {
            super(in, new ThrowingSubReaderWrapper(settings));
            this.settings = settings;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new RandomExceptionDirectoryReaderWrapper(in, settings);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

}
