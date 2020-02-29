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

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.MessageSupplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class NewShardIndexer {

    private static final Logger logger = LogManager.getLogger(NewShardIndexer.class);

    /**
     * Executes bulk item requests and handles request execution exceptions.
     *
     * @return {@code true} if all the requests completed on this thread and the listener was invoked, {@code false} if a requests triggered
     * a mapping update that will finish and invoke the rescheduler on a different thread
     */
    public static boolean indexBulkItemRequests(BulkExecutionContext context, UpdateHelper updateHelper, LongSupplier nowInMillisSupplier,
                                                MappingUpdatePerformer mappingUpdater, Consumer<ActionListener<Void>> waitForMappingUpdate,
                                                Runnable rescheduler, Runnable writeScheduler, ThreadPool threadPool) throws Exception {
        while (context.hasMoreOperationsToIndex()) {
            if (executeBulkItemRequest(context, updateHelper, nowInMillisSupplier, mappingUpdater, waitForMappingUpdate,
                rescheduler, writeScheduler, threadPool) == false) {
                // We are waiting for a mapping update on another thread. Return false to indicate that not
                // all requests were completed.
                return false;
            }
            assert context.isInitial(); // either completed and moved to next or reset
        }
        return true;
    }

    /**
     * Executes bulk item request and handles request execution exceptions.
     *
     * @return {@code true} if request completed on this thread and the listener was invoked, {@code false} if the request triggered
     * a mapping update that will finish and invoke the rescheduler on a different thread
     */
    static boolean executeBulkItemRequest(BulkExecutionContext context, UpdateHelper updateHelper, LongSupplier nowInMillisSupplier,
                                          MappingUpdatePerformer mappingUpdater, Consumer<ActionListener<Void>> waitForMappingUpdate,
                                          Runnable rescheduler, Runnable writeScheduler, ThreadPool threadPool) throws Exception {
        final DocWriteRequest.OpType opType = context.getCurrent().opType();

        final UpdateHelper.Result updateResult;
        if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateRequest updateRequest = (UpdateRequest) context.getCurrent();
            try {
                updateResult = updateHelper.prepare(updateRequest, context.getPrimary(), nowInMillisSupplier);
            } catch (Exception failure) {
                // we may fail translating a update to index or delete operation
                // we use index result to communicate failure while translating update request
                final Engine.Result result =
                    new Engine.IndexResult(failure, updateRequest.version());
                context.setRequestToIndex(updateRequest);
                context.markOperationAsIndexed(result);
                context.markAsIndexedComplete(context.getExecutionResult(), null);
                return true;
            }
            // execute translated update request
            switch (updateResult.getResponseResult()) {
                case CREATED:
                case UPDATED:
                    IndexRequest indexRequest = updateResult.action();
                    IndexMetaData metaData = context.getPrimary().indexSettings().getIndexMetaData();
                    MappingMetaData mappingMd = metaData.mapping();
                    indexRequest.process(metaData.getCreationVersion(), mappingMd, updateRequest.concreteIndex());
                    context.setRequestToIndex(indexRequest);
                    break;
                case DELETED:
                    context.setRequestToIndex(updateResult.action());
                    break;
                case NOOP:
                    context.markOperationAsNoOp(updateResult.action());
                    context.markAsIndexedComplete(context.getExecutionResult(), null);
                    return true;
                default:
                    throw new IllegalStateException("Illegal update operation " + updateResult.getResponseResult());
            }
        } else {
            context.setRequestToIndex(context.getCurrent());
            updateResult = null;
        }

        assert context.getRequestToIndex() != null; // also checks that we're in TRANSLATED state

        final IndexShard primary = context.getPrimary();
        final long version = context.getRequestToIndex().version();
        final boolean isDelete = context.getRequestToIndex().opType() == DocWriteRequest.OpType.DELETE;
        final IndexShard.OperationContext operationContext;
        if (isDelete) {
            try {
                final DeleteRequest request = context.getRequestToIndex();
                operationContext = primary.startDeleteOperationOnPrimary(version, request.id(), request.versionType(),
                    request.ifSeqNo(), request.ifPrimaryTerm());
            } catch (InternalEngine.CouldNotAcquireVersionSemaphore e) {
                handleVersionAcquireFailure(context, rescheduler, writeScheduler, threadPool);
                return false;
            }
        } else {
            try {
                final IndexRequest request = context.getRequestToIndex();
                operationContext = primary.startIndexOperationOnPrimary(version, request.versionType(), new SourceToParse(
                        request.index(), request.id(), request.source(), request.getContentType(), request.routing()),
                    request.ifSeqNo(), request.ifPrimaryTerm(), request.getAutoGeneratedTimestamp(), request.isRetry());
            } catch (InternalEngine.CouldNotAcquireVersionSemaphore e) {
                handleVersionAcquireFailure(context, rescheduler, writeScheduler, threadPool);
                return false;
            }

        }
        if (operationContext.isDone()) {
            Engine.Result result = operationContext.getResult();
            try (IndexShard.OperationContext toClose = operationContext) {
                if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                    try {
                        primary.mapperService().merge(MapperService.SINGLE_MAPPING_NAME,
                            new CompressedXContent(result.getRequiredMappingUpdate(), XContentType.JSON, ToXContent.EMPTY_PARAMS),
                            MapperService.MergeReason.MAPPING_UPDATE_PREFLIGHT);
                    } catch (Exception e) {
                        logger.info(() -> new ParameterizedMessage("{} mapping update rejected by primary", primary.shardId()), e);
                        onComplete(exceptionToResult(e, primary, isDelete, version), context, updateResult, null);
                        return true;
                    }

                    mappingUpdater.updateMappings(result.getRequiredMappingUpdate(), primary.shardId(),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(Void v) {
                                context.markAsRequiringMappingUpdate();
                                waitForMappingUpdate.accept(
                                    ActionListener.runAfter(new ActionListener<>() {
                                        @Override
                                        public void onResponse(Void v) {
                                            assert context.requiresWaitingForMappingUpdate();
                                            context.resetForExecutionForRetry();
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            context.failOnMappingUpdate(e);
                                        }
                                    }, rescheduler)
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                onComplete(exceptionToResult(e, primary, isDelete, version), context, updateResult, null);
                                // Requesting mapping update failed, so we don't have to wait for a cluster state update
                                assert context.isInitial();
                                rescheduler.run();
                            }
                        });
                    return false;
                } else {
                    onComplete(result, context, updateResult, null);
                    return true;
                }
            }
        } else {
            try {
                operationContext.perform();
                onComplete(operationContext.getResult(), context, updateResult, operationContext);
            } finally {
                if (operationContext.isDone()) {
                    operationContext.close();
                }
            }
            return true;
        }
    }

    private static void handleVersionAcquireFailure(BulkExecutionContext context, Runnable rescheduler, Runnable writeScheduler,
                                                    ThreadPool threadPool) {
        context.markAsRequiringMappingUpdate();
        context.resetForExecutionForRetry();
        if (context.hasPendingWrites()) {
            writeScheduler.run();
        } else {
            threadPool.schedule(rescheduler, TimeValue.timeValueMillis(20), ThreadPool.Names.GENERIC);
        }
    }

    private static Engine.Result exceptionToResult(Exception e, IndexShard primary, boolean isDelete, long version) {
        return isDelete ? primary.getFailedDeleteResult(e, version) : primary.getFailedIndexResult(e, version);
    }

    private static void onComplete(Engine.Result r, BulkExecutionContext context, UpdateHelper.Result updateResult,
                                   IndexShard.OperationContext indexContext) {
        context.markOperationAsIndexed(r);
        final DocWriteRequest<?> docWriteRequest = context.getCurrent();
        final DocWriteRequest.OpType opType = docWriteRequest.opType();
        final boolean isUpdate = opType == DocWriteRequest.OpType.UPDATE;
        final BulkItemResponse executionResult = context.getExecutionResult();
        final boolean isFailed = executionResult.isFailed();
        if (isUpdate && isFailed && isConflictException(executionResult.getFailure().getCause())
            && context.getRetryCounter() < ((UpdateRequest) docWriteRequest).retryOnConflict()) {
            context.resetForExecutionForRetry();
            return;
        }
        final BulkItemResponse response;
        if (isUpdate) {
            response = processUpdateResponse((UpdateRequest) docWriteRequest, context.getConcreteIndex(), executionResult, updateResult);
        } else {
            if (isFailed) {
                final Exception failure = executionResult.getFailure().getCause();
                final MessageSupplier messageSupplier = () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                    context.getPrimary().shardId(), opType.getLowercase(), docWriteRequest);
                if (isConflictException(failure)) {
                    logger.trace(messageSupplier, failure);
                } else {
                    logger.debug(messageSupplier, failure);
                }
            }
            response = executionResult;
        }
        context.markAsIndexedComplete(response, indexContext);
        assert context.isInitial();
    }

    private static boolean isConflictException(final Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException;
    }

    private static BulkItemResponse processUpdateResponse(final UpdateRequest updateRequest, final String concreteIndex,
                                                          BulkItemResponse operationResponse, final UpdateHelper.Result translate) {
        final BulkItemResponse response;
        if (operationResponse.isFailed()) {
            response = new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, operationResponse.getFailure());
        } else {
            final DocWriteResponse.Result translatedResult = translate.getResponseResult();
            final UpdateResponse updateResponse;
            if (translatedResult == DocWriteResponse.Result.CREATED || translatedResult == DocWriteResponse.Result.UPDATED) {
                final IndexRequest updateIndexRequest = translate.action();
                final IndexResponse indexResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(indexResponse.getShardInfo(), indexResponse.getShardId(),
                    indexResponse.getId(), indexResponse.getSeqNo(), indexResponse.getPrimaryTerm(),
                    indexResponse.getVersion(), indexResponse.getResult());

                if (updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) {
                    final BytesReference indexSourceAsBytes = updateIndexRequest.source();
                    final Tuple<XContentType, Map<String, Object>> sourceAndContent =
                        XContentHelper.convertToMap(indexSourceAsBytes, true, updateIndexRequest.getContentType());
                    updateResponse.setGetResult(UpdateHelper.extractGetResult(updateRequest, concreteIndex,
                        indexResponse.getSeqNo(), indexResponse.getPrimaryTerm(),
                        indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                }
            } else if (translatedResult == DocWriteResponse.Result.DELETED) {
                final DeleteResponse deleteResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(deleteResponse.getShardInfo(), deleteResponse.getShardId(),
                    deleteResponse.getId(), deleteResponse.getSeqNo(), deleteResponse.getPrimaryTerm(),
                    deleteResponse.getVersion(), deleteResponse.getResult());

                final GetResult getResult = UpdateHelper.extractGetResult(updateRequest, concreteIndex,
                    deleteResponse.getSeqNo(), deleteResponse.getPrimaryTerm(), deleteResponse.getVersion(),
                    translate.updatedSourceAsMap(), translate.updateSourceContentType(), null);

                updateResponse.setGetResult(getResult);
            } else {
                throw new IllegalArgumentException("unknown operation type: " + translatedResult);
            }
            response = new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, updateResponse);
        }
        return response;
    }
}
