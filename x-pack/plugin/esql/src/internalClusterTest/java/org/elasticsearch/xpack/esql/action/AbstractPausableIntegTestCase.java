/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** A pausable testcase. Subclasses extend this testcase to simulate slow running queries.
 *
 * Uses the evaluation of a runtime field in the mappings "pause_me" of type long, along
 * with a custom script language "pause", and semaphore "scriptPermits", to block execution.
 */
public abstract class AbstractPausableIntegTestCase extends AbstractEsqlIntegTestCase {

    protected static final Semaphore scriptPermits = new Semaphore(0);

    protected int pageSize = -1;

    protected int numberOfDocs = -1;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), PausableFieldPlugin.class);
    }

    protected int pageSize() {
        if (pageSize == -1) {
            pageSize = between(10, 100);
        }
        return pageSize;
    }

    protected int numberOfDocs() {
        if (numberOfDocs == -1) {
            numberOfDocs = between(4 * pageSize(), 5 * pageSize());
        }
        return numberOfDocs;
    }

    @Before
    public void setupIndex() throws IOException {
        assumeTrue("requires query pragmas", canUseQueryPragmas());

        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("pause_me");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", "pause").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate("test").setSettings(indexSettings(1, 0)).setMapping(mapping.endObject()).get();

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numberOfDocs(); i++) {
            bulk.add(prepareIndex("test").setId(Integer.toString(i)).setSource("foo", i));
        }
        bulk.get();
        /*
         * forceMerge so we can be sure that we don't bump into tiny
         * segments that finish super quickly and cause us to report strange
         * statuses when we expect "starting".
         */
        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(1).get();
        /*
         * Double super extra paranoid check that force merge worked. It's
         * failed to reduce the index to a single segment and caused this test
         * to fail in very difficult to debug ways. If it fails again, it'll
         * trip here. Or maybe it won't! And we'll learn something. Maybe
         * it's ghosts.
         */
        SegmentsStats stats = client().admin().indices().prepareStats("test").get().getPrimaries().getSegments();
        if (stats.getCount() != 1L) {
            fail(Strings.toString(stats));
        }
    }

    public static class PausableFieldPlugin extends AbstractPauseFieldPlugin {
        @Override
        protected boolean onWait() throws InterruptedException {
            return scriptPermits.tryAcquire(1, TimeUnit.MINUTES);
        }
    }
}
