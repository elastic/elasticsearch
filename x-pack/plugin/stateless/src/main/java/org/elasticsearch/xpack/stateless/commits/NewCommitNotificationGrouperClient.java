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

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationResponse;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class NewCommitNotificationGrouperClient {
    private final Object mutex = new Object();
    private final Client client;
    private final Map<ShardId, ConcurrentRequestsGrouper> concurrentRequestsGroupers = new HashMap<>();

    NewCommitNotificationGrouperClient(Client client) {
        this.client = client;
    }

    void sendRequest(NewCommitNotificationRequest request, ActionListener<NewCommitNotificationResponse> listener) {
        // TODO: instead of grouping all concurrent requests take into account the generation used for notifications and notify the listener
        // if a newer notification comes before an old one and union the results of (old + new) when the old notification response comes
        // back. Additionally, this only concerns the deletion logic, we should decouple this and only group responses for deletions.

        synchronized (mutex) {
            var concurrentRequestsGrouper = concurrentRequestsGroupers.computeIfAbsent(request.shardId(), ConcurrentRequestsGrouper::new);
            listener = concurrentRequestsGrouper.groupConcurrentRequests(listener);
        }

        client.execute(TransportNewCommitNotificationAction.TYPE, request, listener);
    }

    private class ConcurrentRequestsGrouper {
        private final SubscribableListener<NewCommitNotificationResponse> finalListener = new SubscribableListener<>();
        private final List<NewCommitNotificationResponse> responses = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        private int inFlightRequests = 0;
        private final ShardId shardId;

        ConcurrentRequestsGrouper(ShardId shardId) {
            this.shardId = shardId;
        }

        ActionListener<NewCommitNotificationResponse> groupConcurrentRequests(ActionListener<NewCommitNotificationResponse> listener) {
            assert Thread.holdsLock(mutex);

            inFlightRequests++;
            finalListener.addListener(listener);

            return new ActionListener<>() {
                @Override
                public void onResponse(NewCommitNotificationResponse response) {
                    boolean allOutstandingRequestFinished = onListenerNotification();
                    responses.add(response);
                    if (allOutstandingRequestFinished) {
                        notifyListeners();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    boolean allOutstandingRequestFinished = onListenerNotification();
                    addException(e);
                    if (allOutstandingRequestFinished) {
                        notifyListeners();
                    }
                }
            };
        }

        private boolean onListenerNotification() {
            boolean allOutstandingRequestFinished = false;
            synchronized (mutex) {
                if (--inFlightRequests == 0) {
                    allOutstandingRequestFinished = true;
                    var removedGrouper = concurrentRequestsGroupers.remove(shardId);
                    assert removedGrouper == this;
                }
            }
            return allOutstandingRequestFinished;
        }

        private void addException(Exception e) {
            final var firstException = exceptionRef.compareAndExchange(null, e);
            if (firstException != null && firstException != e) {
                firstException.addSuppressed(e);
            }
        }

        void notifyListeners() {
            if (exceptionRef.get() != null) {
                finalListener.onFailure(exceptionRef.get());
            } else {
                NewCommitNotificationResponse finalResponse;
                if (responses.size() == 1) {
                    finalResponse = responses.get(0);
                } else {
                    var responsesUnion = new HashSet<PrimaryTermAndGeneration>();
                    for (var response : responses) {
                        responsesUnion.addAll(response.getUsedPrimaryTermAndGenerations());
                    }
                    finalResponse = new NewCommitNotificationResponse(Collections.unmodifiableSet(responsesUnion));
                }
                finalListener.onResponse(finalResponse);
            }
        }
    }
}
