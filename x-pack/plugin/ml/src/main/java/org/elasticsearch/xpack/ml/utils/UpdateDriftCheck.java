/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Logs a rate-limited invariant violation when an update changes a config field that the update request does not name.
 * Only field names are logged, never values, which may carry queries or credentials.
 */
public final class UpdateDriftCheck {

    private static final String ANALYSIS_CONFIG = Job.ANALYSIS_CONFIG.getPreferredName();

    private final String configType;
    private final ToXContent.Params params;
    private final Set<String> expandedContainers;
    private final Map<String, String> configPathByUpdateKey;
    private final Set<String> derivedPaths;
    private final MlInvariantLogger invariantLogger = new MlInvariantLogger();

    private UpdateDriftCheck(
        String configType,
        ToXContent.Params params,
        Set<String> expandedContainers,
        Map<String, String> configPathByUpdateKey,
        Set<String> derivedPaths
    ) {
        this.configType = configType;
        this.params = params;
        this.expandedContainers = expandedContainers;
        this.configPathByUpdateKey = configPathByUpdateKey;
        this.derivedPaths = derivedPaths;
    }

    public static UpdateDriftCheck forDatafeeds() {
        return new UpdateDriftCheck(
            "datafeed",
            // Same params as the persisted form (DatafeedConfigProvider.TO_XCONTENT_PARAMS).
            new ToXContent.MapParams(Map.of(ToXContentParams.FOR_INTERNAL_STORAGE, "true")),
            Set.of(),
            Map.of(),
            // Rewritten from the request's security headers and by the credential transitions, not by the update body.
            Set.of(DatafeedConfig.HEADERS.getPreferredName(), DatafeedConfig.CLOUD_INTERNAL_CREDENTIAL.getPreferredName())
        );
    }

    public static UpdateDriftCheck forJobs() {
        // A job update names these under analysis_config; clear_job_finish_time acts on finished_time.
        return new UpdateDriftCheck(
            "job",
            ToXContent.EMPTY_PARAMS,
            Set.of(ANALYSIS_CONFIG),
            Map.of(
                JobUpdate.DETECTORS.getPreferredName(),
                analysisConfigPath(JobUpdate.DETECTORS.getPreferredName()),
                AnalysisConfig.CATEGORIZATION_FILTERS.getPreferredName(),
                analysisConfigPath(AnalysisConfig.CATEGORIZATION_FILTERS.getPreferredName()),
                AnalysisConfig.PER_PARTITION_CATEGORIZATION.getPreferredName(),
                analysisConfigPath(AnalysisConfig.PER_PARTITION_CATEGORIZATION.getPreferredName()),
                AnalysisConfig.MODEL_PRUNE_WINDOW.getPreferredName(),
                analysisConfigPath(AnalysisConfig.MODEL_PRUNE_WINDOW.getPreferredName()),
                JobUpdate.CLEAR_JOB_FINISH_TIME.getPreferredName(),
                Job.FINISHED_TIME.getPreferredName()
            ),
            Set.of()
        );
    }

    private static String analysisConfigPath(String field) {
        return ANALYSIS_CONFIG + "." + field;
    }

    /** Never throws: a config that cannot be rendered is logged at DEBUG and skipped. */
    public void check(Logger logger, String id, ToXContentObject before, ToXContentObject after, ToXContentObject update) {
        try {
            Set<String> changed = unrequestedChanges(before, after, update);
            if (changed.isEmpty() == false) {
                invariantLogger.violated(
                    logger,
                    MlInvariantLogger.Kind.UPDATE_CHANGED_UNREQUESTED_FIELD,
                    configType + "/" + id,
                    "Update of " + configType + " [" + id + "] changed fields the request does not name: " + changed
                );
            }
        } catch (IOException | RuntimeException e) {
            logger.debug(() -> "Could not compare " + configType + " [" + id + "] before and after update", e);
        }
    }

    private Set<String> unrequestedChanges(ToXContentObject before, ToXContentObject after, ToXContentObject update) throws IOException {
        Map<String, Object> beforeFields = flatten(toMap(before, params));
        Map<String, Object> afterFields = flatten(toMap(after, params));

        Set<String> excused = new HashSet<>(derivedPaths);
        for (String updateKey : toMap(update, ToXContent.EMPTY_PARAMS).keySet()) {
            excused.add(configPathByUpdateKey.getOrDefault(updateKey, updateKey));
        }

        Set<String> paths = new HashSet<>(beforeFields.keySet());
        paths.addAll(afterFields.keySet());
        Set<String> changed = new TreeSet<>();
        for (String path : paths) {
            if (excused.contains(path) == false && Objects.equals(beforeFields.get(path), afterFields.get(path)) == false) {
                changed.add(path);
            }
        }
        return changed;
    }

    /** Replaces each expanded container by its children, keyed {@code container.child}. */
    private Map<String, Object> flatten(Map<String, Object> fields) {
        Map<String, Object> flat = new HashMap<>();
        for (Map.Entry<String, Object> field : fields.entrySet()) {
            if (expandedContainers.contains(field.getKey()) && field.getValue() instanceof Map<?, ?> children) {
                for (Map.Entry<?, ?> child : children.entrySet()) {
                    flat.put(field.getKey() + "." + child.getKey(), child.getValue());
                }
            } else {
                flat.put(field.getKey(), field.getValue());
            }
        }
        return flat;
    }

    private static Map<String, Object> toMap(ToXContentObject value, ToXContent.Params params) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            value.toXContent(builder, params);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }
}
