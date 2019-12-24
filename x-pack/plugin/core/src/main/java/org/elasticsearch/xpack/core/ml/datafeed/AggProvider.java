/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

class AggProvider implements Writeable, ToXContentObject {

    private static final Logger logger = LogManager.getLogger(AggProvider.class);

    private Exception parsingException;
    private AggregatorFactories.Builder parsedAggs;
    private Map<String, Object> aggs;

    static AggProvider fromXContent(XContentParser parser, boolean lenient) throws IOException {
        Map<String, Object> aggs = parser.mapOrdered();
        AggregatorFactories.Builder parsedAggs = null;
        Exception exception = null;
        try {
            if (aggs.isEmpty()) {
                throw new Exception("aggs cannot be empty");
            }
            parsedAggs = XContentObjectTransformer.aggregatorTransformer(parser.getXContentRegistry()).fromMap(aggs);
        } catch(Exception ex) {
            if (ex.getCause() instanceof IllegalArgumentException) {
                ex = (Exception)ex.getCause();
            }
            exception = ex;
            if (lenient) {
                logger.warn(Messages.DATAFEED_CONFIG_AGG_BAD_FORMAT, ex);
            } else {
                throw ExceptionsHelper.badRequestException(Messages.DATAFEED_CONFIG_AGG_BAD_FORMAT, ex);
            }
        }
        return new AggProvider(aggs, parsedAggs, exception);
    }

    static AggProvider fromParsedAggs(AggregatorFactories.Builder parsedAggs) throws IOException {
        return parsedAggs == null ?
            null :
            new AggProvider(
                XContentObjectTransformer.aggregatorTransformer(NamedXContentRegistry.EMPTY).toMap(parsedAggs),
                parsedAggs,
                null);
    }

    static AggProvider fromStream(StreamInput in) throws IOException {
        return new AggProvider(in.readMap(), in.readOptionalWriteable(AggregatorFactories.Builder::new), in.readException());
    }

    AggProvider(Map<String, Object> aggs, AggregatorFactories.Builder parsedAggs, Exception parsingException) {
        this.aggs = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(aggs, "[aggs] must not be null")));
        this.parsedAggs = parsedAggs;
        this.parsingException = parsingException;
    }

    AggProvider(AggProvider other) {
        this.aggs = new LinkedHashMap<>(other.aggs);
        this.parsedAggs = other.parsedAggs;
        this.parsingException = other.parsingException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(aggs);
        out.writeOptionalWriteable(parsedAggs);
        out.writeException(parsingException);
    }

    public Exception getParsingException() {
        return parsingException;
    }

    AggregatorFactories.Builder getParsedAggs() {
        return parsedAggs;
    }

    public Map<String, Object> getAggs() {
        return aggs;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        AggProvider that = (AggProvider) other;

        return Objects.equals(this.aggs, that.aggs)
            && Objects.equals(this.parsedAggs, that.parsedAggs)
            && Objects.equals(this.parsingException, that.parsingException);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggs, parsedAggs, parsingException);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(aggs);
        return builder;
    }
}
