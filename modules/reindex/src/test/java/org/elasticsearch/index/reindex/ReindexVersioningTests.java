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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.get.GetResponse;

import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.elasticsearch.index.VersionType.EXTERNAL;
import static org.elasticsearch.index.VersionType.INTERNAL;


public class ReindexVersioningTests extends ReindexTestCase {
    private static final int SOURCE_VERSION = 4;
    private static final int OLDER_VERSION = 1;
    private static final int NEWER_VERSION = 10;

    public void testExternalVersioningCreatesWhenAbsentAndSetsVersion() throws Exception {
        setupSourceAbsent();
        assertThat(reindexExternal(), matcher().created(1));
        assertDest("source", SOURCE_VERSION);
    }

    public void testExternalVersioningUpdatesOnOlderAndSetsVersion() throws Exception {
        setupDestOlder();
        assertThat(reindexExternal(), matcher().updated(1));
        assertDest("source", SOURCE_VERSION);
    }

    public void testExternalVersioningVersionConflictsOnNewer() throws Exception {
        setupDestNewer();
        assertThat(reindexExternal(), matcher().versionConflicts(1));
        assertDest("dest", NEWER_VERSION);
    }

    public void testInternalVersioningCreatesWhenAbsent() throws Exception {
        setupSourceAbsent();
        assertThat(reindexInternal(), matcher().created(1));
        assertDest("source", 1);
    }

    public void testInternalVersioningUpdatesOnOlder() throws Exception {
        setupDestOlder();
        assertThat(reindexInternal(), matcher().updated(1));
        assertDest("source", OLDER_VERSION + 1);
    }

    public void testInternalVersioningUpdatesOnNewer() throws Exception {
        setupDestNewer();
        assertThat(reindexInternal(), matcher().updated(1));
        assertDest("source", NEWER_VERSION + 1);
    }

    public void testCreateCreatesWhenAbsent() throws Exception {
        setupSourceAbsent();
        assertThat(reindexCreate(), matcher().created(1));
        assertDest("source", 1);
    }

    public void testCreateVersionConflictsOnOlder() throws Exception {
        setupDestOlder();
        assertThat(reindexCreate(), matcher().versionConflicts(1));
        assertDest("dest", OLDER_VERSION);
    }

    public void testCreateVersionConflictsOnNewer() throws Exception {
        setupDestNewer();
        assertThat(reindexCreate(), matcher().versionConflicts(1));
        assertDest("dest", NEWER_VERSION);
    }

    /**
     * Perform a reindex with EXTERNAL versioning which has "refresh" semantics.
     */
    private BulkByScrollResponse reindexExternal() {
        ReindexRequestBuilder reindex =  reindex().source("source").destination("dest").abortOnVersionConflict(false);
        reindex.destination().setVersionType(EXTERNAL);
        return reindex.get();
    }

    /**
     * Perform a reindex with INTERNAL versioning which has "overwrite" semantics.
     */
    private BulkByScrollResponse reindexInternal() {
        ReindexRequestBuilder reindex =  reindex().source("source").destination("dest").abortOnVersionConflict(false);
        reindex.destination().setVersionType(INTERNAL);
        return reindex.get();
    }

    /**
     * Perform a reindex with CREATE OpType which has "create" semantics.
     */
    private BulkByScrollResponse reindexCreate() {
        ReindexRequestBuilder reindex =  reindex().source("source").destination("dest").abortOnVersionConflict(false);
        reindex.destination().setOpType(CREATE);
        return reindex.get();
    }

    private void setupSourceAbsent() throws Exception {
        indexRandom(true, client().prepareIndex("source").setId("test").setVersionType(EXTERNAL)
                .setVersion(SOURCE_VERSION).setSource("foo", "source"));

        assertEquals(SOURCE_VERSION, client().prepareGet("source", "test").get().getVersion());
    }

    private void setupDest(int version) throws Exception {
        setupSourceAbsent();
        indexRandom(true, client().prepareIndex("dest").setId("test").setVersionType(EXTERNAL)
                .setVersion(version).setSource("foo", "dest"));

        assertEquals(version, client().prepareGet("dest", "test").get().getVersion());
    }

    private void setupDestOlder() throws Exception {
        setupDest(OLDER_VERSION);
    }

    private void setupDestNewer() throws Exception {
        setupDest(NEWER_VERSION);
    }

    private void assertDest(String fooValue, int version) {
        GetResponse get = client().prepareGet("dest", "test").get();
        assertEquals(fooValue, get.getSource().get("foo"));
        assertEquals(version, get.getVersion());
    }
}
