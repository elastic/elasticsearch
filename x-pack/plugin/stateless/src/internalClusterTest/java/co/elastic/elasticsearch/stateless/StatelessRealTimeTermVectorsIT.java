/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.termvectors.EnsureDocsSearchableAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.termvectors.TransportShardMultiTermsVectorAction;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessRealTimeTermVectorsIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    public void testNonExistentDocsDoNotRefresh() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();

        final int numDocs = randomIntBetween(1, 20);
        createIndexWithTermVectorMappings(1, 1);
        indexTestDocuments(numDocs);
        if (randomBoolean()) {
            refresh(INDEX_NAME);
        }
        TestCounters testCounters = installTestCounters(false);

        boolean multiTermVectorRequest = randomBoolean(); // else send single term vector API requests
        if (multiTermVectorRequest) {
            var multiTermVectorsBuilder = client().prepareMultiTermVectors();
            for (int i = 0; i < numDocs; i++) {
                multiTermVectorsBuilder = multiTermVectorsBuilder.add(INDEX_NAME, String.valueOf(1_000_000 + i));
            }
            MultiTermVectorsResponse response = multiTermVectorsBuilder.get();
            assertThat(response.getResponses().length, equalTo(numDocs));
            for (int i = 0; i < numDocs; i++) {
                final var termVectorsResponse = response.getResponses()[i].getResponse();
                assertThat(termVectorsResponse, notNullValue());
                assertThat(termVectorsResponse.getIndex(), equalTo(INDEX_NAME));
                assertThat(termVectorsResponse.isExists(), equalTo(false));
            }
        } else {
            for (int i = 0; i < numDocs; i++) {
                ActionFuture<TermVectorsResponse> termVector = client().termVectors(
                    new TermVectorsRequest(INDEX_NAME, String.valueOf(1_000_000 + i))
                );
                TermVectorsResponse termVectorsResponse = termVector.actionGet();
                assertThat(termVectorsResponse, notNullValue());
                assertThat(termVectorsResponse.getIndex(), equalTo(INDEX_NAME));
                assertThat(termVectorsResponse.isExists(), equalTo(false));
            }
        }

        assertThat(testCounters.ensureDocsActionsSent().get(), equalTo(multiTermVectorRequest ? 1 : numDocs));
        assertThat(testCounters.unpromotableRefreshesSent().get(), equalTo(0));
    }

    public void testNonRealTimeDocsDoNotRefresh() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();

        final int numDocs = randomIntBetween(1, 20);
        createIndexWithTermVectorMappings(1, 1);
        indexTestDocuments(numDocs);
        boolean refreshIndexBeforehand = randomBoolean();
        if (refreshIndexBeforehand) {
            refresh(INDEX_NAME); // the docs will be found in the search node
        }
        TestCounters testCounters = installTestCounters(false);

        boolean multiTermVectorRequest = randomBoolean(); // else send single term vector API requests
        if (multiTermVectorRequest) {
            var multiTermVectorsBuilder = client().prepareMultiTermVectors();
            for (int i = 0; i < numDocs; i++) {
                var termVectorsRequest = new TermVectorsRequest(INDEX_NAME, String.valueOf(i));
                termVectorsRequest = termVectorsRequest.realtime(false);
                multiTermVectorsBuilder = multiTermVectorsBuilder.add(termVectorsRequest);
            }
            MultiTermVectorsResponse response = multiTermVectorsBuilder.get();
            assertThat(response.getResponses().length, equalTo(numDocs));
            for (int i = 0; i < numDocs; i++) {
                final var termVectorsResponse = response.getResponses()[i].getResponse();
                assertThat(termVectorsResponse, notNullValue());
                assertThat(termVectorsResponse.getIndex(), equalTo(INDEX_NAME));
                assertThat(termVectorsResponse.isExists(), equalTo(refreshIndexBeforehand));
            }
        } else {
            for (int i = 0; i < numDocs; i++) {
                var termVectorsRequest = new TermVectorsRequest(INDEX_NAME, String.valueOf(i));
                termVectorsRequest = termVectorsRequest.realtime(false);
                TermVectorsResponse termVectorsResponse = client().termVectors(termVectorsRequest).actionGet();
                assertThat(termVectorsResponse, notNullValue());
                assertThat(termVectorsResponse.getIndex(), equalTo(INDEX_NAME));
                assertThat(termVectorsResponse.isExists(), equalTo(refreshIndexBeforehand));
            }
        }

        assertThat(testCounters.ensureDocsActionsSent().get(), equalTo(0));
        assertThat(testCounters.unpromotableRefreshesSent().get(), equalTo(0));
    }

    private enum TestDocsType {
        DOCS_EXIST_IN_LIVE_VERSION_MAP_ONLY,             // Docs exist in the live version map only, but not in the archive yet
        DOCS_EXIST_IN_LIVE_VERSION_MAP_ARCHIVE_ONLY,     // Docs exist in the live version map archive only, and not in the live version map
        DOCS_SEARCHABLE_AND_NOT_IN_LIVE_VERSION_MAP      // Docs do not exist in the live version map nor the archive, and are searchable
    }

    public void testDocsInLiveVersionMapRefreshUpToTwoTimes() throws Exception {
        doTestDocsInLiveVersionMapRefresh(TestDocsType.DOCS_EXIST_IN_LIVE_VERSION_MAP_ONLY, false);
    }

    public void testDocsInLiveVersionMapArchiveRefreshUpToOneTime() throws Exception {
        doTestDocsInLiveVersionMapRefresh(TestDocsType.DOCS_EXIST_IN_LIVE_VERSION_MAP_ARCHIVE_ONLY, false);
    }

    public void testDocsNotInLiveVersionMapDoNotRefresh() throws Exception {
        doTestDocsInLiveVersionMapRefresh(TestDocsType.DOCS_SEARCHABLE_AND_NOT_IN_LIVE_VERSION_MAP, false);
    }

    public void testUpdatedDocsInLiveVersionMapRefreshUpToTwoTimes() throws Exception {
        doTestDocsInLiveVersionMapRefresh(TestDocsType.DOCS_EXIST_IN_LIVE_VERSION_MAP_ONLY, true);
    }

    public void testUpdatedDocsInLiveVersionMapArchiveRefreshUpToOneTime() throws Exception {
        doTestDocsInLiveVersionMapRefresh(TestDocsType.DOCS_EXIST_IN_LIVE_VERSION_MAP_ARCHIVE_ONLY, true);
    }

    public void testUpdatedDocsNotInLiveVersionMapDoNotRefresh() throws Exception {
        doTestDocsInLiveVersionMapRefresh(TestDocsType.DOCS_SEARCHABLE_AND_NOT_IN_LIVE_VERSION_MAP, true);
    }

    public void doTestDocsInLiveVersionMapRefresh(TestDocsType testDocsType, boolean updatedDocs) throws Exception {
        startMasterAndIndexNode();
        startSearchNode();

        final int numDocs = randomIntBetween(1, 20);
        createIndexWithTermVectorMappings(1, 1);
        indexTestDocuments(numDocs);

        // If true, we test that the term vectors API will refresh appropriately depending on whether the second (not the first) version
        // of the updated documents is in the live version map (i.e., not yet searchable) of the indexing shard.
        if (updatedDocs) {
            if (randomBoolean()) {
                refresh(INDEX_NAME); // first version exists on the search nodes, and on the live version map archive of the indexing node
                if (randomBoolean()) {
                    refresh(INDEX_NAME); // first version exists only on the search nodes
                }
            }
            updateTestDocuments(numDocs); // second version exists in the live version map (but not yet in the archive) of the indexing node
        }

        switch (testDocsType) {
            case DOCS_EXIST_IN_LIVE_VERSION_MAP_ONLY -> {
                // Do not refresh, so that the docs are in the live version map only and not in the archive yet
                // We can expect up to two maximum refreshes in the test for docs to be moved out of the live version map (and the archive)
            }
            case DOCS_EXIST_IN_LIVE_VERSION_MAP_ARCHIVE_ONLY -> {
                // Refresh once so that docs move out of the live version map into the live version map archive
                // We can expect one refresh in the test to ensure that the docs are not found in the live version map archive
                refresh(INDEX_NAME);
            }
            case DOCS_SEARCHABLE_AND_NOT_IN_LIVE_VERSION_MAP -> {
                // Refresh twice, so that the docs are not found in the live version map at all (neither in the archive)
                // No additional refreshes are expected in the test.
                refresh(INDEX_NAME);
                refresh(INDEX_NAME);
            }
        }

        TestCounters testCounters = installTestCounters(false);

        boolean multiTermVectorRequest = randomBoolean(); // else send single term vector API requests
        if (multiTermVectorRequest) {
            var multiTermVectorsBuilder = client().prepareMultiTermVectors();
            for (int i = 0; i < numDocs; i++) {
                multiTermVectorsBuilder = multiTermVectorsBuilder.add(INDEX_NAME, String.valueOf(i));
            }
            MultiTermVectorsResponse response = multiTermVectorsBuilder.get();
            assertThat(response.getResponses().length, equalTo(numDocs));
            for (int i = 0; i < numDocs; i++) {
                final var termVectorsResponse = response.getResponses()[i].getResponse();
                assertThat(termVectorsResponse, notNullValue());
                assertThat(termVectorsResponse.getIndex(), equalTo(INDEX_NAME));
                assertThat(termVectorsResponse.isExists(), equalTo(true));
                Fields fields = termVectorsResponse.getFields();
                assertThat(fields.size(), equalTo(1));
                Terms terms = fields.terms("field");
                assertThat(terms.size(), greaterThan(0L));
            }
        } else {
            for (int i = 0; i < numDocs; i++) {
                ActionFuture<TermVectorsResponse> termVector = client().termVectors(new TermVectorsRequest(INDEX_NAME, String.valueOf(i)));
                TermVectorsResponse termVectorsResponse = termVector.actionGet();
                assertThat(termVectorsResponse, notNullValue());
                assertThat(termVectorsResponse.getIndex(), equalTo(INDEX_NAME));
                assertThat(termVectorsResponse.isExists(), equalTo(true));
                Fields fields = termVectorsResponse.getFields();
                assertThat(fields.size(), equalTo(1));
                Terms terms = fields.terms("field");
                assertThat(terms.size(), greaterThan(0L));
            }
        }

        assertThat(testCounters.ensureDocsActionsSent().get(), equalTo(multiTermVectorRequest ? 1 : numDocs));
        // Assert that the number of refreshes is maximum 2 depending on the circumstances
        if (multiTermVectorRequest) {
            if (testDocsType == TestDocsType.DOCS_EXIST_IN_LIVE_VERSION_MAP_ONLY
                || testDocsType == TestDocsType.DOCS_EXIST_IN_LIVE_VERSION_MAP_ARCHIVE_ONLY) {
                // The multi term vector request triggered a refresh
                assertThat(testCounters.unpromotableRefreshesSent().get(), equalTo(1));
            } else {
                // The multi term vector request did not need to trigger a refresh
                assertThat(testCounters.unpromotableRefreshesSent().get(), equalTo(0));
            }
        } else {
            if (testDocsType == TestDocsType.DOCS_EXIST_IN_LIVE_VERSION_MAP_ONLY) {
                // The first two term vector requests triggered a refresh
                assertThat(testCounters.unpromotableRefreshesSent().get(), equalTo(Math.min(numDocs, 2)));
            } else if (testDocsType == TestDocsType.DOCS_EXIST_IN_LIVE_VERSION_MAP_ARCHIVE_ONLY) {
                // The first term vector request triggered a refresh
                assertThat(testCounters.unpromotableRefreshesSent().get(), equalTo(1));
            } else {
                // The term vector requests did not need to trigger a refresh
                assertThat(testCounters.unpromotableRefreshesSent().get(), equalTo(0));
            }
        }
    }

    public void testRefreshFails() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();

        final int numDocs = randomIntBetween(1, 20);
        createIndexWithTermVectorMappings(1, 1);
        indexTestDocuments(numDocs);
        refresh(INDEX_NAME); // the docs will be in the live version map archive and the test will incur one refresh
        TestCounters testCounters = installTestCounters(true);

        boolean multiTermVectorRequest = randomBoolean(); // else send single term vector API requests
        if (multiTermVectorRequest) {
            var multiTermVectorsBuilder = client().prepareMultiTermVectors();
            for (int i = 0; i < numDocs; i++) {
                multiTermVectorsBuilder = multiTermVectorsBuilder.add(INDEX_NAME, String.valueOf(i));
            }
            MultiTermVectorsResponse response = multiTermVectorsBuilder.get();
            for (int i = 0; i < numDocs; i++) {
                final var multiTermVectorsItemResponse = response.getResponses()[i];
                assertTrue(multiTermVectorsItemResponse.isFailed());
                assertNull(multiTermVectorsItemResponse.getResponse());
                final var failure = multiTermVectorsItemResponse.getFailure();
                assertNotNull(failure);
                checkTestInducedException(failure.getCause());
            }
        } else {
            final int i = randomIntBetween(0, numDocs - 1);
            ActionFuture<TermVectorsResponse> termVector = client().termVectors(new TermVectorsRequest(INDEX_NAME, String.valueOf(i)));
            Exception exception = expectThrows(Exception.class, () -> termVector.get());
            checkTestInducedException(exception);
        }

        assertThat(testCounters.ensureDocsActionsSent().get(), equalTo(1));
        assertThat(testCounters.unpromotableRefreshesSent().get(), equalTo(1));
    }

    protected static void checkTestInducedException(Exception exception) throws Exception {
        Throwable testInducedCause = exception;
        while (testInducedCause != null && testInducedCause.getMessage().contains("test induced") == false) {
            testInducedCause = testInducedCause.getCause();
        }
        if (testInducedCause == null) {
            throw exception;
        }
    }

    protected static final String INDEX_NAME = "test";

    protected record TestCounters(Supplier<Integer> ensureDocsActionsSent, Supplier<Integer> unpromotableRefreshesSent) {};

    protected TestCounters installTestCounters(boolean unpromotableRefreshesException) {
        final AtomicInteger ensureDocsActionsSent = new AtomicInteger();
        final AtomicInteger unpromotableRefreshesSent = new AtomicInteger();
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            boolean indexNode = mockTransportService.getLocalNode().hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName());
            boolean searchNode = mockTransportService.getLocalNode().hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName());
            mockTransportService.addRequestHandlingBehavior(
                TransportShardMultiTermsVectorAction.TYPE.name() + "[s]",
                (handler, request, channel, task) -> {
                    assert indexNode == false : "indexing nodes should not handle multi terms vector actions";
                    handler.messageReceived(request, channel, task);
                }
            );
            mockTransportService.addRequestHandlingBehavior(TermVectorsAction.NAME, (handler, request, channel, task) -> {
                assert indexNode == false : "indexing nodes should not handle terms vector actions";
                handler.messageReceived(request, channel, task);
            });
            mockTransportService.addRequestHandlingBehavior(
                EnsureDocsSearchableAction.TYPE.name() + "[s]",
                (handler, request, channel, task) -> {
                    assert searchNode == false : "search nodes should not ensure docs searchable actions";
                    ensureDocsActionsSent.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                }
            );
            mockTransportService.addRequestHandlingBehavior(
                TransportUnpromotableShardRefreshAction.NAME + "[u]",
                (handler, request, channel, task) -> {
                    assert indexNode == false : "index nodes should not handle unpromotable shard refresh actions";
                    unpromotableRefreshesSent.incrementAndGet();
                    if (unpromotableRefreshesException) {
                        EsRejectedExecutionException ex = new EsRejectedExecutionException("test induced", false);
                        channel.sendResponse(new RemoteTransportException("test induced", ex));
                    } else {
                        handler.messageReceived(request, channel, task);
                    }
                }
            );
        }
        return new TestCounters(ensureDocsActionsSent::get, unpromotableRefreshesSent::get);
    }

    protected void createIndexWithTermVectorMappings(int shards, int replicas) throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            prepareCreate(INDEX_NAME).setMapping(mapping)
                .setSettings(indexSettings(shards, replicas).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1))
        );
    }

    protected void indexTestDocuments(int numDocs) {
        for (int i = 0; i < numDocs; i++) {
            final var response = prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("field", "foo bar " + i).get();
            assertEquals(1, response.getVersion());
        }
    }

    protected void updateTestDocuments(int numDocs) {
        for (int i = 0; i < numDocs; i++) {
            final var response = client().prepareUpdate(INDEX_NAME, String.valueOf(i)).setDoc("field", "foo bar " + (i + 1)).get();
            assertEquals(2, response.getVersion());
        }
    }
}
