/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.crossproject.NoMatchingProjectException;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Includes helper functions for validating the ESQL query provided to a datafeed.
 */
public class EsqlDatafeedQueryValidator {

    /**
     * Returns the summary count field name that the ESQL query must output, or {@code null} if it is
     * not required. The field is only required when the job configures a {@code summary_count_field_name}
     * and the datafeed's delayed data check is enabled.
     */
    public static String requiredSummaryCountField(DatafeedConfig datafeed, Job job) {
        String summaryCountField = job.getAnalysisConfig().getSummaryCountFieldName();
        DelayedDataCheckConfig delayedDataCheckConfig = datafeed.getDelayedDataCheckConfig();
        boolean delayedDataCheckEnabled = delayedDataCheckConfig != null && delayedDataCheckConfig.isEnabled();
        return (Strings.hasText(summaryCountField) && delayedDataCheckEnabled) ? summaryCountField : null;
    }

    /**
     * Validates an ESQL datafeed query by executing {@code esqlQuery | LIMIT 0} under the supplied
     * security headers. This surfaces any query problem — invalid syntax, a query that fails to run,
     * or missing required output columns ({@code timeField} and, when non-null, {@code summaryCountField}).
     * Calls {@code listener.onResponse(true)} on success or when the target index does not exist;
     * calls {@code listener.onFailure} for all other problems.
     */
    public void validateQuery(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String timeField,
        String summaryCountField,
        ActionListener<Boolean> listener
    ) {
        String limitZeroQuery = esqlQuery + " | LIMIT 0";

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> {
            try {
                checkRequiredColumns(response.response().columns(), timeField, summaryCountField);
                listener.onResponse(Boolean.TRUE);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                // Tolerate a missing index: the datafeed may be created before the index exists.
                listener.onResponse(Boolean.TRUE);
            } else {
                listener.onFailure(e);
            }
        });

        executeEsqlQueryAsync(client, limitZeroQuery, headers, projectRouting, responseListener);
    }

    /**
     * Probe run before minting a CPS internal credential: executes {@code esqlQuery | LIMIT 0} under
     * the caller's credential to confirm access. Does NOT check output columns (that is done by
     * {@link #validateQuery}). Tolerates {@link NoMatchingProjectException} (a project may be linked
     * later) and {@link IndexNotFoundException} (the index may be created later).
     * Calls {@code listener.onResponse(null)} on success or for those tolerated failures, and
     * {@code listener.onFailure} for all other problems.
     */
    public void validateAccessForMint(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        ActionListener<Void> listener
    ) {
        String limitZeroQuery = esqlQuery + " | LIMIT 0";

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> listener.onResponse(null), e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof NoMatchingProjectException) {
                // Flat-world (unqualified) routing matched no project right now; a project may be
                // linked later. Defer to runtime — consistent with the classic SearchRequest probe.
                listener.onResponse(null);
            } else if (cause instanceof IndexNotFoundException) {
                // The target index may not exist yet; tolerate and defer to runtime.
                listener.onResponse(null);
            } else {
                listener.onFailure(e);
            }
        });

        executeEsqlQueryAsync(client, limitZeroQuery, headers, projectRouting, responseListener);
    }

    static void checkRequiredColumns(List<? extends ColumnInfo> columns, String timeField, String requiredSummaryCountField) {
        boolean foundTimeField = false;
        boolean foundSummaryCountField = requiredSummaryCountField == null;
        for (ColumnInfo column : columns) {
            String name = column.name();
            if (timeField.equals(name)) {
                foundTimeField = true;
            }
            if (requiredSummaryCountField != null && requiredSummaryCountField.equals(name)) {
                foundSummaryCountField = true;
            }
        }
        List<String> missingColumns = new ArrayList<>();
        if (foundTimeField == false) {
            missingColumns.add(timeField);
        }
        if (foundSummaryCountField == false) {
            missingColumns.add(requiredSummaryCountField);
        }
        if (missingColumns.isEmpty() == false) {
            throw new IllegalArgumentException(
                "ESQL query response is missing the required columns: "
                    + String.join(", ", missingColumns)
                    + ". Ensure the query's final projection includes these columns."
            );
        }
    }

    @SuppressWarnings("unchecked")
    protected void executeEsqlQueryAsync(
        Client client,
        String query,
        Map<String, String> headers,
        @Nullable String projectRouting,
        ActionListener<EsqlQueryResponse> listener
    ) {
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = (EsqlQueryRequestBuilder<
            EsqlQueryRequest,
            EsqlQueryResponse>) EsqlQueryRequestBuilder.newRequestBuilder(client).query(query);
        if (projectRouting != null) {
            builder.projectRouting(projectRouting);
        }
        ClientHelper.executeWithHeadersAsync(
            client.threadPool().getThreadContext(),
            headers,
            ML_ORIGIN,
            builder,
            listener,
            (b, l) -> b.execute(l)
        );
    }
}
