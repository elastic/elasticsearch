/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public class MlAnomaliesView {

    private static final Logger logger = LogManager.getLogger(MlAnomaliesView.class);

    public MlAnomaliesView(ViewService viewService) {
        viewService.ensureReservedViewExists(
            Metadata.DEFAULT_PROJECT_ID,
            new View(".ml-anomalies", """
                FROM .ml-anomalies-*, *:.ml-anomalies-* METADATA _index
                | WHERE result_type IN ("bucket", "record", "influencer")
                | EVAL score = COALESCE(anomaly_score, record_score, influencer_score),
                       initial_score = COALESCE(initial_anomaly_score, initial_record_score, initial_influencer_score)
                | KEEP timestamp,
                       event.ingested,
                       job_id,
                       _index,
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
                       processing_time_ms
                """, "View that contains the results of machine learning anomaly detection jobs.", true),
            ActionListener.wrap(
                ack -> logger.info("reserved view [.ml-anomalies] initialized"),
                e -> logger.error("failed to initialize reserved view [.ml-anomalies]: ", e)
            )
        );
    }
}
