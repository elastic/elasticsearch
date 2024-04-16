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
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessBatchedBehavioursIT extends AbstractStatelessIntegTestCase {

    public void testDefaultToNotifyOnlyForUpload() {
        final String indexNode = startMasterAndIndexNode();
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final MockTransportService transportService = MockTransportService.getInstance(indexNode);
        final var requestRef = new AtomicReference<NewCommitNotificationRequest>();
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportNewCommitNotificationAction.NAME + "[u]")) {
                final boolean success = requestRef.compareAndSet(null, (NewCommitNotificationRequest) request);
                assertThat("expect null requestRef, but got " + requestRef.get(), success, is(true));
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final int numberOfRuns = between(1, 5);
        for (int i = 0; i < numberOfRuns; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            refresh(indexName);
            final NewCommitNotificationRequest request = requestRef.getAndSet(null);
            assertThat(request, notNullValue());
            assertThat(request.isUpload(), is(true));
        }
    }

    public void testNewCommitNotificationOnCreation() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder().put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true).build()
        );
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final MockTransportService transportService = MockTransportService.getInstance(indexNode);
        final Queue<NewCommitNotificationRequest> requests = ConcurrentCollections.newQueue();
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportNewCommitNotificationAction.NAME + "[u]")) {
                requests.add((NewCommitNotificationRequest) request);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final int numberOfRuns = between(1, 5);
        for (int i = 0; i < numberOfRuns; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            refresh(indexName);
            assertBusy(() -> {
                final List<NewCommitNotificationRequest> requestList = List.copyOf(requests);
                assertThat(requestList.size(), equalTo(2));
                final var creationNotificationRequest = requestList.get(0);
                final var uploadNotificationRequest = requestList.get(1);
                assertThat(creationNotificationRequest.isUpload(), is(false));
                assertThat(uploadNotificationRequest.isUpload(), is(true));

                assertThat(creationNotificationRequest.getCompoundCommit(), equalTo(uploadNotificationRequest.getCompoundCommit()));
                assertThat(
                    creationNotificationRequest.getBatchedCompoundCommitGeneration(),
                    equalTo(uploadNotificationRequest.getBatchedCompoundCommitGeneration())
                );

                assertThat(
                    creationNotificationRequest.getLatestUploadedBatchedCompoundCommitTermAndGen().generation(),
                    equalTo(creationNotificationRequest.getBatchedCompoundCommitGeneration() - 1L)
                );
                assertThat(
                    uploadNotificationRequest.getLatestUploadedBatchedCompoundCommitTermAndGen().generation(),
                    equalTo(uploadNotificationRequest.getBatchedCompoundCommitGeneration())
                );

                requests.clear();
            });
        }
    }
}
