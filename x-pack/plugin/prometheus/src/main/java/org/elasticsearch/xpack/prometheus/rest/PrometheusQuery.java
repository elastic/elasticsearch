/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * What a client asks one of the Prometheus read endpoints, as its REST handler reads it from the HTTP request. It holds the
 * PromQL query and the selectors as the client sent them: {@link PrometheusQueryTransportAction} parses them, once the request
 * is authorized.
 */
sealed interface PrometheusQuery {

    /** The index pattern the query reads. */
    String index();

    /** {@code /api/v1/query}: the query evaluated at {@code time}. */
    record InstantQuery(String query, String index, Instant time) implements PrometheusQuery {
        public InstantQuery {
            Objects.requireNonNull(query);
            Objects.requireNonNull(index);
            Objects.requireNonNull(time);
        }
    }

    /**
     * {@code /api/v1/query_range}: the query evaluated at each step of {@code [start, end]}. The bounds and the step are parsed
     * with the query, as they are checked against each other.
     */
    record RangeQuery(String query, String index, String start, String end, String step, int limit) implements PrometheusQuery {
        public RangeQuery {
            Objects.requireNonNull(query);
            Objects.requireNonNull(index);
            Objects.requireNonNull(start);
            Objects.requireNonNull(end);
            Objects.requireNonNull(step);
        }
    }

    /** {@code /api/v1/query_exemplars}: the exemplars of the series the query selects within {@code [start, end]}. */
    record ExemplarsQuery(String query, String index, Instant start, Instant end) implements PrometheusQuery {
        public ExemplarsQuery {
            Objects.requireNonNull(query);
            Objects.requireNonNull(index);
            Objects.requireNonNull(start);
            Objects.requireNonNull(end);
        }
    }

    /** {@code /api/v1/series}: the series the selectors select within {@code [start, end]}. */
    record SeriesQuery(String index, List<String> matchSelectors, Instant start, Instant end, int limit) implements PrometheusQuery {
        public SeriesQuery {
            Objects.requireNonNull(index);
            matchSelectors = List.copyOf(matchSelectors);
            Objects.requireNonNull(start);
            Objects.requireNonNull(end);
        }
    }

    /** {@code /api/v1/labels}: the label names of the series the selectors select within {@code [start, end]}. */
    record LabelsQuery(String index, List<String> matchSelectors, Instant start, Instant end, int limit) implements PrometheusQuery {
        public LabelsQuery {
            Objects.requireNonNull(index);
            matchSelectors = List.copyOf(matchSelectors);
            Objects.requireNonNull(start);
            Objects.requireNonNull(end);
        }
    }

    /** {@code /api/v1/label/{name}/values}: the values of a label in the series the selectors select within {@code [start, end]}. */
    record LabelValuesQuery(String labelName, String index, List<String> matchSelectors, Instant start, Instant end, int limit)
        implements
            PrometheusQuery {
        public LabelValuesQuery {
            Objects.requireNonNull(labelName);
            Objects.requireNonNull(index);
            matchSelectors = List.copyOf(matchSelectors);
            Objects.requireNonNull(start);
            Objects.requireNonNull(end);
        }
    }

    /** {@code /api/v1/metadata}: the metadata of the metrics, or of {@code metric} only if it isn't {@code null}. */
    record MetadataQuery(String index, String metric, int limit, int limitPerMetric) implements PrometheusQuery {
        public MetadataQuery {
            Objects.requireNonNull(index);
        }
    }
}
