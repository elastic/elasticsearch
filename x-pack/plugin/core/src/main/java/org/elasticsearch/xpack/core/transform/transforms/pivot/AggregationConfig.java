/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue.Level;
import org.elasticsearch.xpack.core.deprecation.LoggingDeprecationAccumulationHandler;
import org.elasticsearch.xpack.core.transform.TransformDeprecations;
import org.elasticsearch.xpack.core.transform.TransformMessages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/*
 * Wrapper for the aggregations config part of a composite aggregation.
 *
 * For now just wraps aggregations from composite aggs.
 *
 */
public class AggregationConfig implements Writeable, ToXContentObject {
    private static final Logger logger = LogManager.getLogger(AggregationConfig.class);

    // we store the query in 2 formats: the raw format and the parsed format
    private final Map<String, Object> source;
    private final AggregatorFactories.Builder aggregations;

    public AggregationConfig(final Map<String, Object> source, AggregatorFactories.Builder aggregations) {
        this.source = source;
        this.aggregations = aggregations;
    }

    public AggregationConfig(final StreamInput in) throws IOException {
        source = in.readMap();
        aggregations = in.readOptionalWriteable(AggregatorFactories.Builder::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(source);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(source);
        out.writeOptionalWriteable(aggregations);
    }

    public Collection<AggregationBuilder> getAggregatorFactories() {
        return aggregations.getAggregatorFactories();
    }

    public Collection<PipelineAggregationBuilder> getPipelineAggregatorFactories() {
        return aggregations.getPipelineAggregatorFactories();
    }

    public Collection<String> getUsedNames() {
        if (aggregations == null) {
            return Collections.emptyList();
        }
        List<String> usedNames = new ArrayList<>();
        getAggregatorFactories().forEach(agg -> addAggNames("", agg, usedNames));
        getPipelineAggregatorFactories().forEach(agg -> addAggNames("", agg, usedNames));
        return usedNames;
    }

    private static void addAggNames(String namePrefix, AggregationBuilder aggregationBuilder, Collection<String> names) {
        if (aggregationBuilder.getSubAggregations().isEmpty() && aggregationBuilder.getPipelineAggregations().isEmpty()) {
            names.add(namePrefix + aggregationBuilder.getName());
            return;
        }

        String newNamePrefix = namePrefix + aggregationBuilder.getName() + ".";
        aggregationBuilder.getSubAggregations().forEach(agg -> addAggNames(newNamePrefix, agg, names));
        aggregationBuilder.getPipelineAggregations().forEach(agg -> addAggNames(newNamePrefix, agg, names));
    }

    private static void addAggNames(String namePrefix, PipelineAggregationBuilder pipelineAggregationBuilder, Collection<String> names) {
        names.add(namePrefix + pipelineAggregationBuilder.getName());
    }

    public static AggregationConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        NamedXContentRegistry registry = parser.getXContentRegistry();
        Map<String, Object> source = parser.mapOrdered();
        AggregatorFactories.Builder aggregations = null;

        if (source.isEmpty()) {
            if (lenient) {
                logger.warn(TransformMessages.TRANSFORM_CONFIGURATION_PIVOT_NO_AGGREGATION);
            } else {
                throw new IllegalArgumentException(TransformMessages.TRANSFORM_CONFIGURATION_PIVOT_NO_AGGREGATION);
            }
        } else {
            try {
                aggregations = aggregationsFromXContent(source, registry, LoggingDeprecationHandler.INSTANCE);
            } catch (Exception e) {
                if (lenient) {
                    logger.warn(TransformMessages.LOG_TRANSFORM_CONFIGURATION_BAD_AGGREGATION, e);
                } else {
                    throw e;
                }
            }
        }
        return new AggregationConfig(source, aggregations);
    }

    private static AggregatorFactories.Builder aggregationsFromXContent(
        Map<String, Object> source,
        NamedXContentRegistry namedXContentRegistry,
        DeprecationHandler deprecationHandler
    ) throws IOException {
        AggregatorFactories.Builder aggregations = null;

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(source);
        XContentParser sourceParser = XContentType.JSON.xContent()
            .createParser(namedXContentRegistry, deprecationHandler, BytesReference.bytes(xContentBuilder).streamInput());
        sourceParser.nextToken();
        aggregations = AggregatorFactories.parseAggregators(sourceParser);

        return aggregations;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, aggregations);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final AggregationConfig that = (AggregationConfig) other;

        return Objects.equals(this.source, that.source) && Objects.equals(this.aggregations, that.aggregations);
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (aggregations == null) {
            validationException = addValidationError("pivot.aggregations must not be null", validationException);
        }
        return validationException;
    }

    public void checkForDeprecations(String id, NamedXContentRegistry namedXContentRegistry, Consumer<DeprecationIssue> onDeprecation) {
        LoggingDeprecationAccumulationHandler deprecationLogger = new LoggingDeprecationAccumulationHandler();

        try {
            aggregationsFromXContent(source, namedXContentRegistry, deprecationLogger);
        } catch (IOException e) {
            onDeprecation.accept(
                new DeprecationIssue(
                    Level.CRITICAL,
                    "Transform [" + id + "]: " + TransformMessages.LOG_TRANSFORM_CONFIGURATION_BAD_AGGREGATION,
                    TransformDeprecations.AGGS_BREAKING_CHANGES_URL,
                    e.getMessage(),
                    false,
                    null
                )
            );
        }

        deprecationLogger.getDeprecations().forEach(deprecationMessage -> {
            onDeprecation.accept(
                new DeprecationIssue(
                    Level.CRITICAL,
                    "Transform [" + id + "] uses deprecated aggregation options",
                    TransformDeprecations.AGGS_BREAKING_CHANGES_URL,
                    deprecationMessage,
                    false,
                    null
                )
            );
        });

    }
}
