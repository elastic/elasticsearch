/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.transforms.common.AbstractCompositeAggFunction;
import org.elasticsearch.xpack.transform.transforms.common.DocumentConversionUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * The pivot transform function. This continually searches and pivots results according to the passed {@link PivotConfig}
 */
public class Pivot extends AbstractCompositeAggFunction {
    private static final Logger logger = LogManager.getLogger(Pivot.class);

    private final PivotConfig config;
    private final SettingsConfig settings;
    private final Version version;

    /**
     * Create a new Pivot function
     * @param config A {@link PivotConfig} describing the function parameters
     * @param settings Any miscellaneous settings for the function
     * @param version The version of the transform
     */
    public Pivot(PivotConfig config, SettingsConfig settings, Version version, Set<String> runtimeFields) {
        super(createCompositeAggregation(config, runtimeFields));
        this.config = config;
        this.settings = settings;
        this.version = version == null ? Version.CURRENT : version;
    }

    @Override
    public void validateConfig(ActionListener<Boolean> listener) {
        for (AggregationBuilder agg : config.getAggregationConfig().getAggregatorFactories()) {
            if (TransformAggregations.isSupportedByTransform(agg.getType()) == false) {
                listener.onFailure(
                    new ValidationException().addValidationError(
                        new ParameterizedMessage("Unsupported aggregation type [{}]", agg.getType()).getFormattedMessage()
                    )
                );
                return;
            }
        }
        listener.onResponse(true);
    }

    @Override
    public List<String> getPerformanceCriticalFields() {
        return config.getGroupConfig().getGroups().values().stream().map(SingleGroupSource::getField).collect(toList());
    }

    @Override
    public void deduceMappings(Client client, SourceConfig sourceConfig, final ActionListener<Map<String, String>> listener) {
        if (Boolean.FALSE.equals(settings.getDeduceMappings())) {
            listener.onResponse(emptyMap());
            return;
        }
        SchemaUtil.deduceMappings(client, config, sourceConfig.getIndex(), sourceConfig.getRuntimeMappings(), listener);
    }

    /**
     * Get the initial page size for this pivot.
     *
     * The page size is the main parameter for adjusting memory consumption. Memory consumption mainly depends on
     * the page size, the type of aggregations and the data. As the page size is the number of buckets we return
     * per page the page size is a multiplier for the costs of aggregating bucket.
     *
     * The user may set a maximum in the {@link PivotConfig#getMaxPageSearchSize()}, but if that is not provided,
     *    the default {@link Transform#DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE} is used.
     *
     * In future we might inspect the configuration and base the initial size on the aggregations used.
     *
     * @return the page size
     */
    @Override
    public int getInitialPageSize() {
        return config.getMaxPageSearchSize() == null ? Transform.DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE : config.getMaxPageSearchSize();
    }

    @Override
    public ChangeCollector buildChangeCollector(String synchronizationField) {
        return CompositeBucketsChangeCollector.buildChangeCollector(config.getGroupConfig().getGroups(), synchronizationField);
    }

    @Override
    protected Map<String, Object> documentTransformationFunction(Map<String, Object> document) {
        return DocumentConversionUtils.removeInternalFields(document);
    }

    @Override
    protected Stream<Map<String, Object>> extractResults(
        CompositeAggregation agg,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats transformIndexerStats,
        TransformProgress transformProgress
    ) {
        // defines how dates are written, if not specified in settings
        // < 7.11 as epoch millis
        // >= 7.11 as string
        // note: it depends on the version when the transform has been created, not the version of the code
        boolean datesAsEpoch = settings.getDatesAsEpochMillis() != null ? settings.getDatesAsEpochMillis()
            : version.onOrAfter(Version.V_7_11_0) ? false
            : true;

        return AggregationResultUtils.extractCompositeAggregationResults(
            agg,
            config.getGroupConfig(),
            config.getAggregationConfig().getAggregatorFactories(),
            config.getAggregationConfig().getPipelineAggregatorFactories(),
            fieldTypeMap,
            transformIndexerStats,
            transformProgress,
            datesAsEpoch
        );
    }

    @Override
    public SearchSourceBuilder buildSearchQueryForInitialProgress(SearchSourceBuilder searchSourceBuilder) {
        BoolQueryBuilder existsClauses = QueryBuilders.boolQuery();

        config.getGroupConfig().getGroups().values().forEach(src -> {
            if (src.getMissingBucket() == false && src.getField() != null) {
                existsClauses.must(QueryBuilders.existsQuery(src.getField()));
            }
        });

        return searchSourceBuilder.query(existsClauses).size(0).trackTotalHits(true);
    }

    private static CompositeAggregationBuilder createCompositeAggregation(PivotConfig config, Set<String> runtimeFields) {
        final CompositeAggregationBuilder compositeAggregation = createCompositeAggregationSources(config, runtimeFields);

        config.getAggregationConfig().getAggregatorFactories().forEach(compositeAggregation::subAggregation);
        config.getAggregationConfig().getPipelineAggregatorFactories().forEach(compositeAggregation::subAggregation);

        return compositeAggregation;
    }

    private static CompositeAggregationBuilder createCompositeAggregationSources(PivotConfig config, Set<String> runtimeFields) {
        CompositeAggregationBuilder compositeAggregation;

        Collection<Entry<String, SingleGroupSource>> groups = GroupByOptimizer.reorderGroups(
            config.getGroupConfig().getGroups(),
            runtimeFields
        );

        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.field(CompositeAggregationBuilder.SOURCES_FIELD_NAME.getPreferredName());
            builder.startArray();

            for (Entry<String, SingleGroupSource> groupBy : groups) {
                builder.startObject();
                builder.startObject(groupBy.getKey());
                builder.field(groupBy.getValue().getType().value(), groupBy.getValue());
                builder.endObject();
                builder.endObject();
            }

            builder.endArray();
            builder.endObject(); // sources
            XContentParser parser = builder.generator()
                .contentType()
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
            compositeAggregation = CompositeAggregationBuilder.PARSER.parse(parser, COMPOSITE_AGGREGATION_NAME);
        } catch (IOException e) {
            throw new RuntimeException(
                TransformMessages.getMessage(TransformMessages.TRANSFORM_FAILED_TO_CREATE_COMPOSITE_AGGREGATION, "pivot"),
                e
            );
        }
        return compositeAggregation;
    }

}
