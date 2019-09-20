/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.xpack.core.transform.TransformMessages;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

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
        out.writeMap(source);
        out.writeOptionalWriteable(aggregations);
    }

    public Collection<AggregationBuilder> getAggregatorFactories() {
        return aggregations.getAggregatorFactories();
    }

    public Collection<PipelineAggregationBuilder> getPipelineAggregatorFactories() {
        return aggregations.getPipelineAggregatorFactories();
    }

    public static AggregationConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        NamedXContentRegistry registry = parser.getXContentRegistry();
        Map<String, Object> source =  parser.mapOrdered();
        AggregatorFactories.Builder aggregations = null;

        if (source.isEmpty()) {
            if (lenient) {
                logger.warn(TransformMessages.TRANSFORM_CONFIGURATION_PIVOT_NO_AGGREGATION);
            } else {
                throw new IllegalArgumentException(TransformMessages.TRANSFORM_CONFIGURATION_PIVOT_NO_AGGREGATION);
            }
        } else {
            try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(source);
                    XContentParser sourceParser = XContentType.JSON.xContent().createParser(registry, LoggingDeprecationHandler.INSTANCE,
                            BytesReference.bytes(xContentBuilder).streamInput())) {
                sourceParser.nextToken();
                aggregations = AggregatorFactories.parseAggregators(sourceParser);
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

    public boolean isValid() {
        return this.aggregations != null;
    }
}
