/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
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

    // we store the query in 2 formats: the raw format and the parsed format, which again is divided into
    // proper aggregations and special functions like count
    private final Map<String, Object> source;
    private final AggregatorFactories.Builder aggregations;
    private final Map<String, String> specialAggregations;

    public AggregationConfig(final Map<String, Object> source,
                             final AggregatorFactories.Builder aggregations,
                             final Map<String, String> specialAggregations) {
        this.source = source;
        this.aggregations = aggregations;
        this.specialAggregations = specialAggregations;
    }

    public AggregationConfig(final StreamInput in) throws IOException {
        source = in.readMap();
        aggregations = in.readOptionalWriteable(AggregatorFactories.Builder::new);
        if (in.getVersion().onOrAfter(Version.CURRENT)) {
            if (in.readBoolean()) {
                specialAggregations = in.readMap(StreamInput::readString, StreamInput::readString);
            } else {
                specialAggregations = null;
            }
        } else {
            specialAggregations = null;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(source);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(source);
        out.writeOptionalWriteable(aggregations);
        if (out.getVersion().onOrAfter(Version.CURRENT)) {
            if (specialAggregations == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeMap(specialAggregations, StreamOutput::writeString, StreamOutput::writeString);
            }
        }
    }

    public Collection<AggregationBuilder> getAggregatorFactories() {
        return aggregations != null ? aggregations.getAggregatorFactories() : Collections.emptySet();
    }

    public Collection<PipelineAggregationBuilder> getPipelineAggregatorFactories() {
        return aggregations != null ? aggregations.getPipelineAggregatorFactories() : Collections.emptySet();
    }

    public Map<String, String> getSpecialAggregations() {
        return specialAggregations != null ? specialAggregations : Collections.emptyMap();
    }

    public static AggregationConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        NamedXContentRegistry registry = parser.getXContentRegistry();
        Map<String, Object> source =  parser.mapOrdered();
        AggregatorFactories.Builder aggregations = null;

        if (source.isEmpty()) {
            if (lenient) {
                logger.warn(DataFrameMessages.DATA_FRAME_TRANSFORM_CONFIGURATION_PIVOT_NO_AGGREGATION);
            } else {
                throw new IllegalArgumentException(DataFrameMessages.DATA_FRAME_TRANSFORM_CONFIGURATION_PIVOT_NO_AGGREGATION);
            }
        } else {
            Map<String, Object> aggregationSource = Aggregations.filterSpecialAggregations(source);
            if (aggregationSource.isEmpty() == false) {
                try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(aggregationSource);
                        XContentParser sourceParser = XContentType.JSON.xContent().createParser(
                                registry,
                                LoggingDeprecationHandler.INSTANCE,
                                BytesReference.bytes(xContentBuilder).streamInput())) {
                    sourceParser.nextToken();
                    aggregations = AggregatorFactories.parseAggregators(sourceParser);
                } catch (Exception e) {
                    if (lenient) {
                        logger.warn(DataFrameMessages.LOG_DATA_FRAME_TRANSFORM_CONFIGURATION_BAD_AGGREGATION, e);
                    } else {
                        throw e;
                    }
                }
            }
        }
        return new AggregationConfig(source, aggregations, Aggregations.getSpecialAggregations(source));
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, aggregations, specialAggregations);
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

        return Objects.equals(this.source, that.source) &&
               Objects.equals(this.aggregations, that.aggregations) &&
               Objects.equals(this.specialAggregations, that.specialAggregations);
    }

    public boolean isValid() {
        return this.aggregations != null || this.specialAggregations != null;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
