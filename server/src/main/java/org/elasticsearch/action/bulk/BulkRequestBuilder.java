/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.DocWriteRequest;
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
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A bulk request holds an ordered {@link IndexRequest}s and {@link DeleteRequest}s and allows to executes
 * it in a single batch.
 */
public class BulkRequestBuilder extends ActionRequestBuilder<BulkRequest, BulkResponse> implements WriteRequestBuilder<BulkRequestBuilder> {
    private final String globalIndex;
    private final List<DocWriteRequest<?>> requests = new ArrayList<>();
    private final List<FramedData> framedDataList = new ArrayList<>();
    private final List<ActionRequestBuilder<?, ?>> requestBuilders = new ArrayList<>();
    private ActiveShardCount waitForActiveShards;
    private TimeValue timeout;
    private String timeoutString;
    private String globalPipeline;
    private String globalRouting;
    private WriteRequest.RefreshPolicy refreshPolicy;

    public BulkRequestBuilder(ElasticsearchClient client, @Nullable String globalIndex) {
        super(client, BulkAction.INSTANCE, null);
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
        framedDataList.add(new FramedData(data, from, length, null, xContentType));
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequestBuilder add(byte[] data, int from, int length, @Nullable String defaultIndex, XContentType xContentType)
        throws Exception {
        framedDataList.add(new FramedData(data, from, length, defaultIndex, xContentType));
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
        return requests.size() + requestBuilders.size() + framedDataList.size();
    }

    public BulkRequestBuilder pipeline(String globalPipeline) {
        this.globalPipeline = globalPipeline;
        return this;
    }

    public BulkRequestBuilder routing(String globalRouting) {
        this.globalRouting = globalRouting;
        return this;
    }

    public BulkRequestBuilder setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public BulkRequest request() {
        if (requests.isEmpty() == false && requestBuilders.isEmpty() == false) {
            throw new IllegalStateException("Must use only requests or request builders within a single bulk request");
        }
        BulkRequest bulkRequest = new BulkRequest(globalIndex);
        try {
            for (ActionRequestBuilder<?, ?> requestBuilder : requestBuilders) {
                ActionRequest request = requestBuilder.request();
                try {
                    bulkRequest.add((DocWriteRequest<?>) request);
                } finally {
                    request.decRef();
                }
            }
            for (DocWriteRequest<?> request : requests) {
                bulkRequest.add(request);
            }
            for (FramedData framedData : framedDataList) {
                try {
                    bulkRequest.add(framedData.data, framedData.from, framedData.length, framedData.defaultIndex, framedData.xContentType);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (waitForActiveShards != null) {
                bulkRequest.waitForActiveShards(waitForActiveShards);
            }
            if (timeout != null) {
                bulkRequest.timeout(timeout);
            }
            if (timeoutString != null) {
                bulkRequest.timeout(timeoutString);
            }
            if (globalPipeline != null) {
                bulkRequest.pipeline(globalPipeline);
            }
            if (globalRouting != null) {
                bulkRequest.routing(globalRouting);
            }
            if (refreshPolicy != null) {
                bulkRequest.setRefreshPolicy(refreshPolicy);
            }
            return bulkRequest;
        } catch (Exception e) {
            bulkRequest.decRef();
            throw e;
        }
    }

    private record FramedData(byte[] data, int from, int length, @Nullable String defaultIndex, XContentType xContentType) {}
}
