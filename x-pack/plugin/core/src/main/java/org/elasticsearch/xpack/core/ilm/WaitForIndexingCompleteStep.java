/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;

/**
 * Waits for the {@link LifecycleSettings#LIFECYCLE_INDEXING_COMPLETE} setting to be set to {@code true} on the leader index,
 * which is then propagated to the follower via CCR. If all shard follow tasks have fatally failed with an
 * {@link IndexNotFoundException} (indicating the leader index was deleted), the condition is also considered met so that
 * ILM can proceed with the unfollow process rather than waiting indefinitely.
 */
final class WaitForIndexingCompleteStep extends AsyncWaitStep {
    private static final Logger logger = LogManager.getLogger(WaitForIndexingCompleteStep.class);

    static final String NAME = "wait-for-indexing-complete";

    WaitForIndexingCompleteStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void evaluateCondition(ProjectState currentState, IndexMetadata indexMetadata, Listener listener, TimeValue masterTimeout) {
        Map<String, String> customIndexMetadata = indexMetadata.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            listener.onResponse(true, null);
            return;
        }

        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(indexMetadata.getSettings());
        if (indexingComplete) {
            listener.onResponse(true, null);
            return;
        }

        FollowStatsAction.StatsRequest request = new FollowStatsAction.StatsRequest();
        request.setIndices(new String[] { indexMetadata.getIndex().getName() });
        getClient(currentState.projectId()).execute(FollowStatsAction.INSTANCE, request, ActionListener.wrap(response -> {
            if (response.getTaskFailures().isEmpty() == false || response.getNodeFailures().isEmpty() == false) {
                logger.debug(
                    "[{}] follow stats request for index [{}] returned partial results due to task/node failures, will retry",
                    getKey().action(),
                    indexMetadata.getIndex().getName()
                );
                listener.onResponse(false, new IndexingNotCompleteInfo());
            } else if (allFollowTasksFailedWithIndexNotFound(response)) {
                logger.info(
                    "[{}] proceeding with unfollow for index [{}] because all shard follow tasks have fatally failed"
                        + " with IndexNotFoundException, the leader index has been deleted",
                    getKey().action(),
                    indexMetadata.getIndex().getName()
                );
                listener.onResponse(true, null);
            } else {
                listener.onResponse(false, new IndexingNotCompleteInfo());
            }
        }, e -> {
            if (e instanceof ResourceNotFoundException) {
                logger.info(
                    "[{}] proceeding with unfollow for index [{}] because no shard follow tasks were found,"
                        + " the leader index may have been deleted",
                    getKey().action(),
                    indexMetadata.getIndex().getName()
                );
                listener.onResponse(true, null);
            } else {
                listener.onResponse(false, new IndexingNotCompleteInfo());
            }
        }));
    }

    /**
     * Returns {@code true} if every shard follow task has a fatal {@link IndexNotFoundException}, indicating the leader index
     * was deleted. The fatal exception may be wrapped in a {@code RemoteTransportException}, so we unwrap before checking.
     * Other fatal exception types are intentionally not treated as "leader deleted" to avoid automatically unfollowing
     * when the issue might be resolvable by the user.
     */
    static boolean allFollowTasksFailedWithIndexNotFound(FollowStatsAction.StatsResponses responses) {
        return responses.getStatsResponses().isEmpty() == false && responses.getStatsResponses().stream().allMatch(r -> {
            var fatalException = r.status().getFatalException();
            return fatalException != null && ExceptionsHelper.unwrapCause(fatalException) instanceof IndexNotFoundException;
        });
    }

    static final class IndexingNotCompleteInfo implements ToXContentObject {

        static final ParseField MESSAGE_FIELD = new ParseField("message");
        static final ParseField INDEXING_COMPLETE = new ParseField(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE);

        private final String message;

        IndexingNotCompleteInfo() {
            this.message = "waiting for the ["
                + LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE
                + "] setting to be set to true on the leader index, it is currently [false]";
        }

        String getMessage() {
            return message;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE_FIELD.getPreferredName(), message);
            builder.field(INDEXING_COMPLETE.getPreferredName(), false);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexingNotCompleteInfo info = (IndexingNotCompleteInfo) o;
            return Objects.equals(getMessage(), info.getMessage());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getMessage());
        }
    }
}
