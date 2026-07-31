/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * Describes where derived metrics are written and how that destination is mapped.
 *
 * <p>Every source data stream gets its own hidden time series data stream so that the derived series of one source can never be
 * confused with, or retained differently from, the series of another. The destination is managed by Elasticsearch: users configure
 * metrics on the source stream and never touch the destination's mappings, dimensions or index mode. It is deliberately not named with
 * a leading dot because it is meant to be queried directly by users; hiding it is enough to keep it out of ordinary wildcard
 * expressions.
 */
public final class DerivedMetricsDestination {

    public static final String DESTINATION_PREFIX = "derived-metrics-";
    public static final String INDEX_PATTERN = DESTINATION_PREFIX + "*";
    public static final String TEMPLATE_NAME = "derived-metrics@template";

    /**
     * Bumped whenever {@link #template()} changes so that existing clusters pick the new definition up.
     */
    public static final long TEMPLATE_VERSION = 1L;

    public static final String TIMESTAMP_FIELD = "@timestamp";
    public static final String METRIC_NAME_FIELD = "metric.name";
    public static final String METRIC_VALUE_FIELD = "metric.value";
    public static final String SOURCE_FIELD = "derived_metrics.source";
    public static final String INTERVAL_FIELD = "derived_metrics.interval";
    public static final String NODE_FIELD = "derived_metrics.node";

    /**
     * User dimensions are written below this prefix. Keeping them in their own namespace means a user dimension can never collide with,
     * or redefine, one of the internal dimensions above.
     */
    public static final String DIMENSION_PREFIX = "dimensions.";

    private static final String MAPPING = """
        {
          "_doc": {
            "dynamic": false,
            "dynamic_templates": [
              {
                "derived_metrics_dimensions": {
                  "path_match": "dimensions.*",
                  "match_mapping_type": "string",
                  "mapping": {
                    "type": "keyword",
                    "time_series_dimension": true
                  }
                }
              }
            ],
            "properties": {
              "@timestamp": {
                "type": "date"
              },
              "metric": {
                "properties": {
                  "name": {
                    "type": "keyword",
                    "time_series_dimension": true
                  },
                  "value": {
                    "type": "double",
                    "time_series_metric": "gauge"
                  }
                }
              },
              "derived_metrics": {
                "properties": {
                  "source": {
                    "type": "keyword",
                    "time_series_dimension": true
                  },
                  "interval": {
                    "type": "keyword",
                    "time_series_dimension": true
                  },
                  "node": {
                    "type": "keyword",
                    "time_series_dimension": true
                  }
                }
              },
              "dimensions": {
                "type": "object",
                "dynamic": true
              }
            }
          }
        }""";

    private static final ComposableIndexTemplate TEMPLATE = buildTemplate();

    private DerivedMetricsDestination() {}

    /**
     * The destination data stream for a source stream at a given interval. Each interval gets its own destination so that retention can
     * differ per resolution, and so a query over one destination never has to filter by interval.
     */
    public static String destinationFor(String sourceDataStream, String interval) {
        return DESTINATION_PREFIX + sourceDataStream + "-" + interval;
    }

    /**
     * Whether the given data stream is itself a derived metrics destination. Used to make sure derived metrics never observe their own
     * writes.
     */
    public static boolean isDestination(String dataStreamName) {
        return dataStreamName.startsWith(DESTINATION_PREFIX);
    }

    /**
     * The managed index template backing every derived metrics destination.
     */
    public static ComposableIndexTemplate template() {
        return TEMPLATE;
    }

    private static ComposableIndexTemplate buildTemplate() {
        Settings settings = Settings.builder()
            .put("index.mode", "time_series")
            .putList("index.routing_path", List.of("metric.name", "derived_metrics.*", "dimensions.*"))
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .build();
        final CompressedXContent mappings;
        try {
            mappings = new CompressedXContent(MAPPING);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to compress the derived metrics destination mapping", e);
        }
        return ComposableIndexTemplate.builder()
            .indexPatterns(List.of(INDEX_PATTERN))
            .template(Template.builder().settings(settings).mappings(mappings))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(true, false))
            .priority(500L)
            .version(TEMPLATE_VERSION)
            .allowAutoCreate(true)
            .metadata(Map.of("description", "managed destination for data stream derived metrics", "managed", true))
            .build();
    }
}
