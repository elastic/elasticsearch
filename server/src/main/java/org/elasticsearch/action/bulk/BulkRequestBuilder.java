/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionRequestLazyBuilder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A bulk request holds an ordered {@link IndexRequest}s and {@link DeleteRequest}s and allows to executes
 * it in a single batch.
 */
public class BulkRequestBuilder extends ActionRequestLazyBuilder<BulkRequest, BulkResponse>
    implements
        WriteRequestBuilder<BulkRequestBuilder> {
    private final String globalIndex;
    /*
     * The following 3 variables hold the list of requests that make up this bulk. Only one can be non-empty. That is, users can't add
     * some IndexRequests and some IndexRequestBuilders. They need to pick one (preferably builders) and stick with it.
     */
    private final List<DocWriteRequest<?>> requests = new ArrayList<>();
    private final List<FramedData> framedData = new ArrayList<>();
    private final List<ActionRequestLazyBuilder<? extends DocWriteRequest<?>, ? extends DocWriteResponse>> requestBuilders =
        new ArrayList<>();
    private ActiveShardCount waitForActiveShards;
    private TimeValue timeout;
    private String timeoutString;
    private String globalPipeline;
    private String globalRouting;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private String refreshPolicyString;
    private Boolean requireAlias;
    private TaskId parentTaskId;

    public BulkRequestBuilder(ElasticsearchClient client, @Nullable String globalIndex) {
        super(client, BulkAction.INSTANCE);
        this.globalIndex = globalIndex;
    }

    public BulkRequestBuilder(ElasticsearchClient client) {
        this(client, null);
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     * @deprecated use {@link #add(IndexRequestBuilder)} instead
     */
    @Deprecated
    public BulkRequestBuilder add(IndexRequest request) {
        requests.add(request);
        return this;
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkRequestBuilder add(IndexRequestBuilder request) {
        requestBuilders.add(request);
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     * @deprecated use {@link #add(DeleteRequestBuilder)} instead
     */
    @Deprecated
    public BulkRequestBuilder add(DeleteRequest request) {
        requests.add(request);
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkRequestBuilder add(DeleteRequestBuilder request) {
        requestBuilders.add(request);
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     * @deprecated use {@link #add(UpdateRequestBuilder)} instead
     */
    @Deprecated
    public BulkRequestBuilder add(UpdateRequest request) {
        requests.add(request);
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public BulkRequestBuilder add(UpdateRequestBuilder request) {
        requestBuilders.add(request);
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequestBuilder add(byte[] data, int from, int length, XContentType xContentType) throws Exception {
        framedData.add(new FramedData(data, from, length, null, xContentType));
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequestBuilder add(byte[] data, int from, int length, @Nullable String defaultIndex, XContentType xContentType) {
        framedData.add(new FramedData(data, from, length, defaultIndex, xContentType));
        return this;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public BulkRequestBuilder setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public BulkRequestBuilder setWaitForActiveShards(final int waitForActiveShards) {
        return setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final BulkRequestBuilder setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final BulkRequestBuilder setTimeout(String timeout) {
        this.timeoutString = timeout;
        return this;
    }

    /**
     * The number of actions currently in the bulk.
     */
    public int numberOfActions() {
        return requests.size() + requestBuilders.size() + framedData.size();
    }

    public BulkRequestBuilder pipeline(String globalPipeline) {
        this.globalPipeline = globalPipeline;
        return this;
    }

    public BulkRequestBuilder routing(String globalRouting) {
        this.globalRouting = globalRouting;
        return this;
    }

    @Override
    public BulkRequestBuilder setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public BulkRequestBuilder setRefreshPolicy(String refreshPolicy) {
        this.refreshPolicyString = refreshPolicy;
        return this;
    }

    public BulkRequestBuilder setRequireAlias(boolean requireAlias) {
        this.requireAlias = requireAlias;
        return this;
    }

    public BulkRequestBuilder setParentTask(TaskId taskId) {
        this.parentTaskId = taskId;
        return this;
    }

    @Override
    public BulkRequest request() {
        validate();
        BulkRequest request = new BulkRequest(globalIndex);
        for (ActionRequestLazyBuilder<? extends DocWriteRequest<?>, ? extends DocWriteResponse> requestBuilder : requestBuilders) {
            DocWriteRequest<?> childRequest = requestBuilder.request();
            request.add(childRequest);
        }
        for (DocWriteRequest<?> childRequest : requests) {
            request.add(childRequest);
        }
        for (FramedData framedData : framedData) {
            try {
                request.add(framedData.data, framedData.from, framedData.length, framedData.defaultIndex, framedData.xContentType);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (waitForActiveShards != null) {
            request.waitForActiveShards(waitForActiveShards);
        }
        if (timeout != null) {
            request.timeout(timeout);
        }
        if (timeoutString != null) {
            request.timeout(timeoutString);
        }
        if (globalPipeline != null) {
            request.pipeline(globalPipeline);
        }
        if (globalRouting != null) {
            request.routing(globalRouting);
        }
        if (refreshPolicy != null) {
            request.setRefreshPolicy(refreshPolicy);
        }
        if (refreshPolicyString != null) {
            request.setRefreshPolicy(refreshPolicyString);
        }
        if (requireAlias != null) {
            request.requireAlias(requireAlias);
        }
        if (parentTaskId != null) {
            request.setParentTask(parentTaskId);
        }
        return request;
    }

    private void validate() {
        if (countNonEmptyLists(requestBuilders, requests, framedData) > 1) {
            throw new IllegalStateException(
                "Must use only request builders, requests, or byte arrays within a single bulk request. Cannot mix and match"
            );
        }
        if (timeout != null && timeoutString != null) {
            throw new IllegalStateException("Must use only one setTimeout method");
        }
        if (refreshPolicy != null && refreshPolicyString != null) {
            throw new IllegalStateException("Must use only one setRefreshPolicy method");
        }
    }

    private int countNonEmptyLists(List<?>... lists) {
        int sum = 0;
        for (List<?> list : lists) {
            if (list.isEmpty() == false) {
                sum++;
            }
        }
        return sum;
    }

    private record FramedData(byte[] data, int from, int length, @Nullable String defaultIndex, XContentType xContentType) {}
}
