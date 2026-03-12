/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.MlTasks;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ModelDownloadTask extends CancellableTask {

    record DownLoadProgress(int totalParts, int downloadedParts) {};

    public record DownloadStatus(DownLoadProgress downloadProgress) implements Task.Status {
        public static final String NAME = "model-download";

        public DownloadStatus(StreamInput in) throws IOException {
            this(new DownLoadProgress(in.readVInt(), in.readVInt()));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("total_parts", downloadProgress.totalParts);
            builder.field("downloaded_parts", downloadProgress.downloadedParts);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(downloadProgress.totalParts);
            out.writeVInt(downloadProgress.downloadedParts);
        }
    }

    private final AtomicReference<DownLoadProgress> downloadProgress = new AtomicReference<>(new DownLoadProgress(0, 0));
    private final String modelId;
    private volatile Exception taskException;

    public ModelDownloadTask(long id, String type, String action, String modelId, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, taskDescription(modelId), parentTaskId, headers);
        this.modelId = modelId;
    }

    void setProgress(int totalParts, int downloadedParts) {
        downloadProgress.set(new DownLoadProgress(totalParts, downloadedParts));
    }

    @Override
    public DownloadStatus getStatus() {
        return new DownloadStatus(downloadProgress.get());
    }

    public String getModelId() {
        return modelId;
    }

    public void setTaskException(Exception exception) {
        this.taskException = exception;
    }

    public Exception getTaskException() {
        return taskException;
    }

    public static String taskDescription(String modelId) {
        return MlTasks.downloadModelTaskDescription(modelId);
    }
}
