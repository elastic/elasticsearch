/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;

import java.util.Objects;

public sealed interface BulkInferenceRequestItem<T extends BaseInferenceActionRequest> permits
    BulkInferenceRequestItem.AbstractBulkInferenceRequestItem {

    TaskType taskType();

    T inferenceRequest();

    BulkInferenceRequestItem<T> withSeqNo(long seqNo);

    Long seqNo();

    static InferenceRequestItem from(InferenceAction.Request request) {
        return new InferenceRequestItem(request);
    }

    static ChatCompletionRequestItem from(UnifiedCompletionAction.Request request) {
        return new ChatCompletionRequestItem(request);
    }

    abstract sealed class AbstractBulkInferenceRequestItem<T extends BaseInferenceActionRequest> implements BulkInferenceRequestItem<T>
        permits InferenceRequestItem, ChatCompletionRequestItem {
        private final T request;
        private final Long seqNo;

        protected AbstractBulkInferenceRequestItem(T request) {
            this(request, null);
        }

        protected AbstractBulkInferenceRequestItem(T request, Long seqNo) {
            this.request = request;
            this.seqNo = seqNo;
        }

        @Override
        public T inferenceRequest() {
            return request;
        }

        @Override
        public Long seqNo() {
            return seqNo;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            AbstractBulkInferenceRequestItem<?> that = (AbstractBulkInferenceRequestItem<?>) o;
            return Objects.equals(request, that.request) && Objects.equals(seqNo, that.seqNo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(request, seqNo);
        }

        @Override
        public TaskType taskType() {
            return request.getTaskType();
        }
    }

    final class InferenceRequestItem extends AbstractBulkInferenceRequestItem<InferenceAction.Request> {
        private InferenceRequestItem(InferenceAction.Request request) {
            this(request, null);
        }

        private InferenceRequestItem(InferenceAction.Request request, Long seqNo) {
            super(request, seqNo);
        }

        @Override
        public InferenceRequestItem withSeqNo(long seqNo) {
            return new InferenceRequestItem(inferenceRequest(), seqNo);
        }
    }

    final class ChatCompletionRequestItem extends AbstractBulkInferenceRequestItem<UnifiedCompletionAction.Request> {

        private ChatCompletionRequestItem(UnifiedCompletionAction.Request request) {
            this(request, null);
        }

        private ChatCompletionRequestItem(UnifiedCompletionAction.Request request, Long seqNo) {
            super(request, seqNo);
        }

        @Override
        public TaskType taskType() {
            return TaskType.CHAT_COMPLETION;
        }

        @Override
        public ChatCompletionRequestItem withSeqNo(long seqNo) {
            return new ChatCompletionRequestItem(inferenceRequest(), seqNo);
        }
    }
}
