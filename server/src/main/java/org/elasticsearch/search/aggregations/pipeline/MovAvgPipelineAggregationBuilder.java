/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.query.CommonTermsQueryBuilder;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * The actual moving_avg aggregation was removed as a breaking change in 8.0. This class exists to provide a friendlier error message
 * if somebody attempts to use the moving_avg aggregation via the compatible-with=7 mechanism.
 *
 * We can remove this class entirely when v7 rest api compatibility is dropped.
 *
 * @deprecated Only for 7.x rest compat
 */
@Deprecated
public class MovAvgPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<MovAvgPipelineAggregationBuilder> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(CommonTermsQueryBuilder.class);
    public static final String MOVING_AVG_AGG_DEPRECATION_MSG = "Moving Average aggregation usage is not supported. "
        + "Use the [moving_fn] aggregation instead.";

    public static ParseField NAME_V7 = new ParseField("moving_avg").withAllDeprecated(MOVING_AVG_AGG_DEPRECATION_MSG)
        .forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.V_7));

    public static final ContextParser<String, MovAvgPipelineAggregationBuilder> PARSER = (parser, name) -> {
        deprecationLogger.compatibleCritical("moving_avg_aggregation", MOVING_AVG_AGG_DEPRECATION_MSG);
        throw new ParsingException(parser.getTokenLocation(), MOVING_AVG_AGG_DEPRECATION_MSG);
    };

    public MovAvgPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME_V7.getPreferredName());
        throw new UnsupportedOperationException("moving_avg is not meant to be used.");
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("moving_avg is not meant to be used.");
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        throw new UnsupportedOperationException("moving_avg is not meant to be used.");
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("moving_avg is not meant to be used.");
    }

    @Override
    protected void validate(ValidationContext context) {
        throw new UnsupportedOperationException("moving_avg is not meant to be used.");
    }

    @Override
    public final String getWriteableName() {
        return null;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }
}
