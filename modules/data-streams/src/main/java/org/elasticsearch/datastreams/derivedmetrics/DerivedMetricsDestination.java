/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
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
     * The managed component template holding the settings a user is allowed to override.
     *
     * <p>They live here rather than on the index template because settings on the index template itself are applied last and therefore
     * win over everything in {@code composed_of} — a setting kept there could not be overridden at all.
     */
    public static final String SETTINGS_COMPONENT_NAME = "derived-metrics@settings";

    /**
     * The optional user-owned component template. It does not have to exist; the index template lists it in
     * {@code ignore_missing_component_templates}. Creating one is how an operator changes the destination's shard count or replica count,
     * following the same convention as {@code logs@custom} and {@code metrics@custom}.
     */
    public static final String CUSTOM_COMPONENT_NAME = "derived-metrics@custom";

    /**
     * Bumped whenever {@link #template()} changes so that existing clusters pick the new definition up.
     */
    public static final long TEMPLATE_VERSION = 6L;

    public static final String TIMESTAMP_FIELD = "@timestamp";
    public static final String METRIC_NAME_FIELD = "metric.name";
    public static final String METRIC_VALUE_FIELD = "metric.value";
    /**
     * Only present on avg gauges, which emit their sum in {@link #METRIC_VALUE_FIELD} and the observation count here so that the mean is
     * a re-aggregatable SUM(value)/SUM(count) rather than an average of averages.
     */
    public static final String METRIC_COUNT_FIELD = "metric.count";
    /**
     * Carries a whole distribution rather than a single number, and so replaces {@link #METRIC_VALUE_FIELD} on histogram metrics rather
     * than joining it. The field carries its own sum, count, min and max, which is why no scalar travels alongside it.
     */
    public static final String METRIC_HISTOGRAM_FIELD = "metric.histogram";
    public static final String SOURCE_FIELD = "derived_metrics.source";
    public static final String INTERVAL_FIELD = "derived_metrics.interval";
    public static final String NODE_FIELD = "derived_metrics.node";
    /**
     * How the metric reduces its observations, so that a consumer can pick the right aggregation without having to go and read the source
     * stream's configuration. Without it {@code metric.value} is a number whose correct combination is unknowable from the data.
     */
    public static final String REDUCTION_FIELD = "derived_metrics.reduction";

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
                  },
                  "count": {
                    "type": "long",
                    "time_series_metric": "gauge"
                  },
                  "histogram": {
                    "type": "exponential_histogram",
                    "time_series_metric": "histogram"
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
                  },
                  "reduction": {
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
     * What a destination name costs on top of itself once it becomes a backing index: {@code .ds-}, a {@code uuuu.MM.dd} date and a six
     * digit generation, each preceded by a separator. A stream that rolls over more than a million times exceeds the six digits, which
     * would leave the last few generations unnameable; that is a limit every data stream shares and is not modelled here.
     */
    private static final int BACKING_INDEX_OVERHEAD = DataStream.BACKING_INDEX_PREFIX.length() + "-uuuu.MM.dd".length() + "-000001"
        .length();

    /**
     * Rejects a source stream whose destination could never be created.
     *
     * <p>Names are concatenated rather than hashed, so a long enough source produces a destination whose backing indices exceed
     * {@link MetadataCreateIndexService#MAX_INDEX_NAME_BYTES}. Without this check the configuration is accepted, nothing is emitted, and
     * the only evidence is a warning in a log — so the failure is moved forward to the moment someone asks for it.
     *
     * @throws IllegalArgumentException if the destination for this source and interval would be unnameable
     */
    public static void validateDestinationName(String sourceDataStream, String interval) {
        String destination = destinationFor(sourceDataStream, interval);
        int bytes = destination.getBytes(StandardCharsets.UTF_8).length + BACKING_INDEX_OVERHEAD;
        if (bytes > MetadataCreateIndexService.MAX_INDEX_NAME_BYTES) {
            throw new IllegalArgumentException(
                "derived metrics cannot be enabled on ["
                    + sourceDataStream
                    + "]: its destination ["
                    + destination
                    + "] would need backing indices of "
                    + bytes
                    + " bytes, over the limit of "
                    + MetadataCreateIndexService.MAX_INDEX_NAME_BYTES
                    + "; shorten the data stream name by at least "
                    + (bytes - MetadataCreateIndexService.MAX_INDEX_NAME_BYTES)
                    + " bytes"
            );
        }
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

    /**
     * The settings a user may override, as a component template. Only the negotiable ones belong here.
     */
    public static ComponentTemplate settingsComponent() {
        return new ComponentTemplate(
            Template.builder()
                .settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1).build())
                .build(),
            TEMPLATE_VERSION,
            Map.of("description", "default settings for data stream derived metrics destinations", "managed", true)
        );
    }

    private static ComposableIndexTemplate buildTemplate() {
        // Only what the feature cannot work without. index.mode and routing_path decide the tsid, which the emitted documents and their
        // _ids depend on, so they stay on the index template where the composition rules put them beyond a user's reach. Shard and replica
        // counts are in the settings component instead, where derived-metrics@custom can override them.
        Settings settings = Settings.builder()
            .put("index.mode", "time_series")
            .putList("index.routing_path", List.of("metric.name", "derived_metrics.*", "dimensions.*"))
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
            .componentTemplates(List.of(SETTINGS_COMPONENT_NAME, CUSTOM_COMPONENT_NAME))
            .ignoreMissingComponentTemplates(List.of(CUSTOM_COMPONENT_NAME))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(true, false))
            .priority(500L)
            .version(TEMPLATE_VERSION)
            .allowAutoCreate(true)
            .metadata(Map.of("description", "managed destination for data stream derived metrics", "managed", true))
            .build();
    }
}
