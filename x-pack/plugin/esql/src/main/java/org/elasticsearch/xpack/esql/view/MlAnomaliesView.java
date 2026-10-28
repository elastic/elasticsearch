/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * The reserved {@code .ml-anomalies} view over the results of anomaly detection jobs. Like the ML annotations index, it is only
 * created once an ML results index exists, to avoid clutter on clusters that never use ML.
 */
public class MlAnomaliesView {

    private static final Logger logger = LogManager.getLogger(MlAnomaliesView.class);

    private static final String RESULTS_INDEX_PREFIX = ".ml-anomalies-";

    public MlAnomaliesView(ViewService viewService) {
        viewService.ensureReservedViewExists(
            Metadata.DEFAULT_PROJECT_ID,
            new View(".ml-anomalies", """
                FROM .ml-anomalies-* METADATA _index
                | WHERE result_type IN ("bucket", "record", "influencer")
                | EVAL score = COALESCE(anomaly_score, record_score, influencer_score),
                       initial_score = COALESCE(initial_anomaly_score, initial_record_score, initial_influencer_score)
                | KEEP timestamp,
                       event.ingested,
                       job_id,
                       result_type,
                       is_interim,
                       score,
                       initial_score,
                       bucket_span,
                       function,
                       function_description,
                       field_name,
                       by_field_name,
                       by_field_value,
                       over_field_name,
                       over_field_value,
                       partition_field_name,
                       partition_field_value,
                       influencer_field_name,
                       influencer_field_value,
                       detector_index,
                       actual,
                       typical,
                       event_count,
                       processing_time_ms,
                       _index
                """, "View that contains the results of machine learning anomaly detection jobs.", true),
            MlAnomaliesView::hasResultsIndex,
            ActionListener.wrap(
                ack -> logger.info("reserved view [.ml-anomalies] initialized"),
                e -> logger.warn("failed to initialize reserved view [.ml-anomalies]: ", e)
            )
        );
    }

    // Views and datasets are not results indices, so they don't count
    static boolean hasResultsIndex(ProjectMetadata project) {
        for (var entry : project.getIndicesLookup().tailMap(RESULTS_INDEX_PREFIX).entrySet()) {
            if (entry.getKey().startsWith(RESULTS_INDEX_PREFIX) == false) {
                return false;
            }
            var type = entry.getValue().getType();
            if (type != IndexAbstraction.Type.VIEW && type != IndexAbstraction.Type.DATASET) {
                return true;
            }
        }
        return false;
    }
}
