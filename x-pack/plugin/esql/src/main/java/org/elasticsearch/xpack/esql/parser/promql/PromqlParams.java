/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;

/**
 * Container for PromQL command parameters:
 * <ul>
 *     <li>time for instant queries</li>
 *     <li>start, end, step for range queries</li>
 * </ul>
 * These can be specified in the {@linkplain org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand PROMQL command} like so:
 * <pre>
 *     # instant query
 *     PROMQL time `2025-10-31T00:00:00Z` (avg(foo))
 *     # range query with explicit start and end
 *     PROMQL start `2025-10-31T00:00:00Z` end `2025-10-31T01:00:00Z` step 1m (avg(foo))
 *     # range query with implicit time bounds, doesn't support calling {@code start()} or {@code end()} functions
 *     PROMQL step 5m (avg(foo))
 * </pre>
 *
 * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#expression-queries">PromQL API documentation</a>
 */
public record PromqlParams(Instant time, Instant start, Instant end, Duration step) {

    private static final String TIME = "time", START = "start", END = "end", STEP = "step";
    private static final Set<String> ALLOWED = Set.of(TIME, START, END, STEP);

    public static PromqlParams parse(EsqlBaseParser.PromqlCommandContext ctx, Source source) {
        Instant time = null;
        Instant start = null;
        Instant end = null;
        Duration step = null;

        Set<String> paramsSeen = new HashSet<>();
        for (EsqlBaseParser.PromqlParamContext paramCtx : ctx.promqlParam()) {
            var paramNameCtx = paramCtx.name;
            String name = paramNameCtx.getText();
            if (paramsSeen.add(name) == false) {
                throw new ParsingException(source(paramNameCtx), "[{}] already specified", name);
            }
            String value = paramCtx.value.getText();
            if (value.startsWith("`") && value.endsWith("`")) {
                value = value.substring(1, value.length() - 1);
            }
            Source valueSource = source(paramCtx.value);
            switch (name) {
                case TIME -> time = PromqlParserUtils.parseDate(valueSource, value);
                case START -> start = PromqlParserUtils.parseDate(valueSource, value);
                case END -> end = PromqlParserUtils.parseDate(valueSource, value);
                case STEP -> {
                    try {
                        step = Duration.ofSeconds(Integer.parseInt(value));
                    } catch (NumberFormatException ignore) {
                        step = PromqlParserUtils.parseDuration(valueSource, value);
                    }
                }
                default -> {
                    String message = "Unknown parameter [{}]";
                    List<String> similar = StringUtils.findSimilar(name, ALLOWED);
                    if (CollectionUtils.isEmpty(similar) == false) {
                        message += ", did you mean " + (similar.size() == 1 ? "[" + similar.get(0) + "]" : "any of " + similar) + "?";
                    }
                    throw new ParsingException(source(paramNameCtx), message, name);
                }
            }
        }

        // Validation logic for time parameters
        if (time != null) {
            if (start != null || end != null || step != null) {
                throw new ParsingException(
                    source,
                    "Specify either [{}] for instant query or [{}}], [{}] or [{}}] for a range query",
                    TIME,
                    STEP,
                    START,
                    END
                );
            }
        } else if (step != null) {
            if (start != null || end != null) {
                if (start == null || end == null) {
                    throw new ParsingException(
                        source,
                        "Parameters [{}] and [{}] must either both be specified or both be omitted for a range query",
                        START,
                        END
                    );
                }
                if (end.isBefore(start)) {
                    throw new ParsingException(
                        source,
                        "invalid parameter \"end\": end timestamp must not be before start time",
                        end,
                        start
                    );
                }
            }
            if (step.isPositive() == false) {
                throw new ParsingException(
                    source,
                    "invalid parameter \"step\": zero or negative query resolution step widths are not accepted. "
                        + "Try a positive integer",
                    step
                );
            }
        } else {
            throw new ParsingException(source, "Parameter [{}] or [{}] is required", STEP, TIME);
        }
        return new PromqlParams(time, start, end, step);
    }
}
