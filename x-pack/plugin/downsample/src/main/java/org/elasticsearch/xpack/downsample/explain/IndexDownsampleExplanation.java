/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.explain;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.downsample.orchestration.IndexTaskDoc;
import org.elasticsearch.xpack.downsample.orchestration.IndexTaskPhase;
import org.elasticsearch.xpack.downsample.orchestration.ShardTaskDoc;
import org.elasticsearch.xpack.downsample.orchestration.ShardTaskStatus;

import java.io.IOException;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * How the downsampling into one index stands. The index metadata gives the downsample status of any downsampled index; the
 * operation and shard task documents of the state index, when there are some for this very index (same uuid), give the details:
 * the phase, the retries and errors, and the progress of every shard.
 *
 * @param index            the downsampled (target) index
 * @param indexUuid        its uuid
 * @param downsampleStatus the downsample status in the index settings
 * @param sourceIndex      the index it is a downsample of, from the index settings
 * @param interval         the downsample interval, from the index settings
 * @param numberOfShards   the number of shards of the index, which is also the number of shard tasks
 * @param operation        the operation in the state index, {@code null} if it was not orchestrated through it (or was cleaned up)
 * @param shards           the shard tasks of the operation that exist, sorted by shard
 * @param now              when the explanation was made, for the elapsed times
 */
public record IndexDownsampleExplanation(
    String index,
    String indexUuid,
    DownsampleTaskStatus downsampleStatus,
    @Nullable String sourceIndex,
    @Nullable String interval,
    int numberOfShards,
    @Nullable IndexTaskDoc operation,
    List<ShardTaskDoc> shards,
    long now
) implements ToXContentObject {

    /**
     * The counters of the shard progress that add up to the progress of the whole index.
     */
    static final List<String> SUMMED_COUNTERS = List.of(
        "total_shard_doc_count",
        "docs_processed",
        "in_num_docs_received",
        "out_num_docs_sent",
        "out_num_docs_indexed",
        "out_num_docs_failed",
        "out_num_docs_skipped_already_indexed"
    );

    static IndexDownsampleExplanation of(IndexMetadata index, @Nullable IndexTaskDoc operation, List<ShardTaskDoc> shards, long now) {
        return new IndexDownsampleExplanation(
            index.getIndex().getName(),
            index.getIndexUUID(),
            IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(index.getSettings()),
            emptyToNull(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.get(index.getSettings())),
            emptyToNull(IndexMetadata.INDEX_DOWNSAMPLE_INTERVAL.get(index.getSettings())),
            index.getNumberOfShards(),
            operation,
            shards,
            now
        );
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    /**
     * Where the downsampling of the index stands as a whole: {@code in_progress}, {@code completed} or {@code failed}.
     */
    public String state() {
        if (operation != null) {
            return switch (operation.phase()) {
                case DONE -> "completed";
                case FAILED -> "failed";
                case CREATE_TARGET, RUN_SHARDS, FINALIZE_SETTINGS, REFRESH, MARK_SUCCESS, FLUSH -> "in_progress";
            };
        }
        return switch (downsampleStatus) {
            case SUCCESS -> "completed";
            case STARTED -> "in_progress";
            case UNKNOWN -> throw new IllegalStateException("index [" + index + "] is not a downsampled index");
        };
    }

    /**
     * The number of shard tasks in each status.
     */
    public Map<ShardTaskStatus, Integer> shardCounts() {
        Map<ShardTaskStatus, Integer> counts = new EnumMap<>(ShardTaskStatus.class);
        for (ShardTaskStatus status : ShardTaskStatus.values()) {
            counts.put(status, 0);
        }
        shards.forEach(shard -> counts.merge(shard.status(), 1, Integer::sum));
        return counts;
    }

    /**
     * The counters of all the shards added up, and the share of the source documents processed so far.
     */
    public Map<String, Object> progress() {
        Map<String, Object> progress = new LinkedHashMap<>();
        for (String counter : SUMMED_COUNTERS) {
            long sum = 0;
            for (ShardTaskDoc shard : shards) {
                if (shard.progress() != null && shard.progress().get(counter) instanceof Number value) {
                    sum += value.longValue();
                }
            }
            progress.put(counter, sum);
        }
        long total = (long) progress.get("total_shard_doc_count");
        if (total > 0) {
            progress.put("docs_processed_percentage", 100.0 * (long) progress.get("docs_processed") / total);
        }
        return progress;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("index_uuid", indexUuid);
        builder.field("state", state());
        builder.field("downsample_status", downsampleStatus.toString());
        builder.field("orchestration", operation == null ? "persistent_task" : "system_index");
        if (sourceIndex != null) {
            builder.field("source_index", sourceIndex);
        }
        if (interval != null) {
            builder.field("interval", interval);
        }
        if (operation != null) {
            operationToXContent(builder, params);
        }
        return builder.endObject();
    }

    private void operationToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("phase", operation.phase().name().toLowerCase(Locale.ROOT));
        builder.field("source_index_uuid", operation.sourceIndexUuid());
        builder.field("config", operation.config(), params);
        builder.timestampFieldsFromUnixEpochMillis("created_at_millis", "created_at", operation.createdAt());
        if (operation.phase().isTerminal() == false) {
            builder.humanReadableField(
                "time_since_created_millis",
                "time_since_created",
                // the operation was created by another node, whose clock may be ahead of this one
                TimeValue.timeValueMillis(Math.max(0, now - operation.createdAt()))
            );
            builder.timestampFieldsFromUnixEpochMillis("deadline_millis", "deadline", operation.deadline());
        }
        builder.field("attempt", operation.attempt());
        if (operation.nextRetryAt() > 0 && operation.phase().isTerminal() == false) {
            builder.timestampFieldsFromUnixEpochMillis("next_retry_at_millis", "next_retry_at", operation.nextRetryAt());
        }
        if (operation.lastError() != null) {
            builder.field("last_error", operation.lastError());
        }
        if (operation.errorHistory().isEmpty() == false) {
            builder.field("error_history", operation.errorHistory());
        }

        builder.startObject("shards_summary");
        builder.field("total", numberOfShards);
        for (Map.Entry<ShardTaskStatus, Integer> count : shardCounts().entrySet()) {
            builder.field(count.getKey().name().toLowerCase(Locale.ROOT), count.getValue());
        }
        builder.endObject();
        builder.field("progress", progress());

        builder.startArray("shards");
        for (ShardTaskDoc shard : shards) {
            builder.startObject();
            builder.field("shard", shard.shard());
            builder.field("status", shard.status().name().toLowerCase(Locale.ROOT));
            if (shard.assignedNode() != null) {
                builder.field("node", shard.assignedNode());
            }
            builder.field("epoch", shard.epoch());
            builder.field("attempt", shard.attempt());
            if (shard.runId() != null) {
                builder.field("run_id", shard.runId());
            }
            if (shard.lastError() != null) {
                builder.field("last_error", shard.lastError());
                builder.field("retriable", shard.retriable());
            }
            if (shard.progress() != null) {
                builder.field("progress", shard.progress());
            }
            builder.endObject();
        }
        builder.endArray();
    }

    /**
     * Whether the operation reached a phase that has shard tasks.
     */
    static boolean hasShardTasks(IndexTaskDoc operation) {
        return operation.targetIndexUuid() != null && operation.phase() != IndexTaskPhase.CREATE_TARGET;
    }
}
