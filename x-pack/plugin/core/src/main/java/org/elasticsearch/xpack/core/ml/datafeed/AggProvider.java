/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
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
    private boolean rewroteAggs;

    static AggProvider fromXContent(XContentParser parser, boolean lenient) throws IOException {
        Map<String, Object> aggs = parser.mapOrdered();
        // NOTE: Always rewrite potentially old date histogram intervals.
        // This should occur in 8.x+ but not 7.x.
        // 7.x is BWC with versions that do not support the new date_histogram fields
        boolean rewroteAggs = false;
        if (lenient) {
            rewroteAggs = rewriteDateHistogramInterval(aggs, false);
        }
        AggregatorFactories.Builder parsedAggs = null;
        Exception exception = null;
        try {
            if (aggs.isEmpty()) {
                throw new Exception("aggs cannot be empty");
            }
            parsedAggs = XContentObjectTransformer.aggregatorTransformer(parser.getXContentRegistry()).fromMap(aggs);
        } catch (Exception ex) {
            if (ex.getCause() instanceof IllegalArgumentException) {
                ex = (Exception) ex.getCause();
            }
            exception = ex;
            if (lenient) {
                logger.warn(Messages.DATAFEED_CONFIG_AGG_BAD_FORMAT, ex);
            } else {
                throw ExceptionsHelper.badRequestException(Messages.DATAFEED_CONFIG_AGG_BAD_FORMAT, ex);
            }
        }
        return new AggProvider(aggs, parsedAggs, exception, rewroteAggs);
    }

    @SuppressWarnings("unchecked")
    static boolean rewriteDateHistogramInterval(Map<String, Object> aggs, boolean inDateHistogram) {
        boolean didRewrite = false;
        if (aggs.containsKey(Histogram.INTERVAL_FIELD.getPreferredName()) && inDateHistogram) {
            Object currentInterval = aggs.remove(Histogram.INTERVAL_FIELD.getPreferredName());
            if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(currentInterval.toString()) != null) {
                aggs.put("calendar_interval", currentInterval.toString());
                didRewrite = true;
            } else if (currentInterval instanceof Number number) {
                aggs.put("fixed_interval", number.longValue() + "ms");
                didRewrite = true;
            } else if (currentInterval instanceof String str) {
                aggs.put("fixed_interval", str);
                didRewrite = true;
            } else {
                throw ExceptionsHelper.badRequestException(
                    Messages.DATAFEED_CONFIG_AGG_BAD_FORMAT,
                    new IllegalArgumentException("unable to parse date_histogram interval parameter")
                );
            }
        }
        for (Map.Entry<String, Object> entry : aggs.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?>) {
                boolean rewrite = rewriteDateHistogramInterval(
                    (Map<String, Object>) entry.getValue(),
                    entry.getKey().equals(DateHistogramAggregationBuilder.NAME)
                );
                didRewrite = didRewrite || rewrite;
            }
        }
        return didRewrite;
    }

    static AggProvider fromParsedAggs(AggregatorFactories.Builder parsedAggs) throws IOException {
        return parsedAggs == null
            ? null
            : new AggProvider(
                XContentObjectTransformer.aggregatorTransformer(NamedXContentRegistry.EMPTY).toMap(parsedAggs),
                parsedAggs,
                null,
                false
            );
    }

    static AggProvider fromStream(StreamInput in) throws IOException {
        return new AggProvider(
            in.readMap(),
            in.readOptionalWriteable(AggregatorFactories.Builder::new),
            in.readException(),
            in.getVersion().onOrAfter(Version.V_8_0_0) ? in.readBoolean() : false
        );
    }

    AggProvider(Map<String, Object> aggs, AggregatorFactories.Builder parsedAggs, Exception parsingException, boolean rewroteAggs) {
        this.aggs = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(aggs, "[aggs] must not be null")));
        this.parsedAggs = parsedAggs;
        this.parsingException = parsingException;
        this.rewroteAggs = rewroteAggs;
    }

    AggProvider(AggProvider other) {
        this.aggs = new LinkedHashMap<>(other.aggs);
        this.parsedAggs = other.parsedAggs;
        this.parsingException = other.parsingException;
        this.rewroteAggs = other.rewroteAggs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(aggs);
        out.writeOptionalWriteable(parsedAggs);
        out.writeException(parsingException);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeBoolean(rewroteAggs);
        }
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

    public boolean isRewroteAggs() {
        return rewroteAggs;
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
            && equalExceptionMessages(this.parsingException, that.parsingException)
            && Objects.equals(this.rewroteAggs, that.rewroteAggs);
    }

    private static boolean equalExceptionMessages(Exception lft, Exception rgt) {
        if (lft == rgt) {
            return true;
        }
        if (lft == null || rgt == null) {
            return false;
        }
        return Objects.equals(lft.getMessage(), rgt.getMessage());
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggs, parsedAggs, parsingException == null ? null : parsingException.getMessage(), rewroteAggs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(aggs);
        return builder;
    }

    @Override
    public String toString() {
        return "AggProvider{"
            + "parsingException="
            + parsingException
            + ", parsedAggs="
            + parsedAggs
            + ", aggs="
            + aggs
            + ", rewroteAggs="
            + rewroteAggs
            + '}';
    }
}
