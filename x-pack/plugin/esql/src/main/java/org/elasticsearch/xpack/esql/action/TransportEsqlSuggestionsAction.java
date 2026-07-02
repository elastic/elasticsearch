/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlSuggestionsResponse.FieldSuggestion;
import org.elasticsearch.xpack.esql.action.suggestions.CursorLocation;
import org.elasticsearch.xpack.esql.action.suggestions.SuggestionBuilder;
import org.elasticsearch.xpack.esql.action.suggestions.SuggestionContext;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

/**
 * Transport handler for {@code POST /_esql/suggestions}.
 *
 * <p>This action runs the <b>coordinator-only</b> completion pipeline: parse the query, translate the
 * cursor offset to a plan position, detect the completion context, and — for statically-resolvable
 * schemas (projection/eval commands) — emit field-name suggestions from the command preceding the
 * cursor.
 *
 * <p><b>Deferred:</b> the data-node visit that populates {@code values}/{@code range} statistics and
 * detects DLS/FLS at the shard level, plus hot/cold shard pruning for wildcard patterns. Those paths
 * require threading a new completion request through {@code ComputeService}; the response shape and
 * warning vocabulary already accommodate them. When {@code includeSampleValues} is requested, or when
 * a single-field literal context is detected, this action still returns the coordinator-only field
 * skeleton so callers get a stable shape rather than an error.
 */
public class TransportEsqlSuggestionsAction extends HandledTransportAction<EsqlSuggestionsRequest, EsqlSuggestionsResponse> {

    private final EsqlParser parser;

    @Inject
    public TransportEsqlSuggestionsAction(TransportService transportService, ActionFilters actionFilters, PlanExecutor planExecutor) {
        super(
            EsqlSuggestionsAction.NAME,
            transportService,
            actionFilters,
            EsqlSuggestionsRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        // A stateless parser is sufficient for coordinator-only completion; it does not consult cluster state.
        this.parser = new EsqlParser(new EsqlConfig(planExecutor.functionRegistry()));
    }

    @Override
    protected void doExecute(Task task, EsqlSuggestionsRequest request, ActionListener<EsqlSuggestionsResponse> listener) {
        ActionListener.completeWith(listener, () -> suggest(parser, request));
    }

    /**
     * Pure coordinator-side completion. Extracted as a static method so it can be unit-tested with a
     * plain {@link EsqlParser} and no transport plumbing.
     */
    static EsqlSuggestionsResponse suggest(EsqlParser parser, EsqlSuggestionsRequest request) {
        LogicalPlan parsed = parser.parseQuery(request.query());
        CursorLocation locations = new CursorLocation(request.query());
        SuggestionContext context = SuggestionContext.detect(parsed, locations, request.cursor());

        Map<String, FieldSuggestion> fields = switch (context.kind()) {
            // For a single-field literal context we only know the field the caret compares against;
            // the actual values/range come from a data-node visit that is deferred, so emit an empty
            // skeleton (the field's type is not statically known on the coordinator here).
            case STRING_LITERAL_EQUALITY, NUMERIC_LITERAL_RANGE -> Map.of();
            // Field-name and pipe positions list the schema of the preceding command, when it is
            // statically resolvable (projection/eval commands). Otherwise the map is empty.
            case FIELD_NAME, PIPE_POSITION -> SuggestionBuilder.fieldsFromSchema(context.schemaSource(parsed));
        };

        return new EsqlSuggestionsResponse(fields, List.of());
    }
}
