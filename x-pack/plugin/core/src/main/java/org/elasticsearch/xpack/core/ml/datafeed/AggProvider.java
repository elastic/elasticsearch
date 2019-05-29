/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
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

public class AggProvider implements Writeable, ToXContentObject {

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
            if (lenient == false) {
                aggs = XContentObjectTransformer.aggregatorTransformer(parser.getXContentRegistry()).toMap(parsedAggs);
            }
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
        if (in.getVersion().onOrAfter(Version.V_6_7_0)) { // Has our bug fix for query/agg providers
            return new AggProvider(in.readMap(), in.readOptionalWriteable(AggregatorFactories.Builder::new), in.readException());
        } else if (in.getVersion().onOrAfter(Version.V_6_6_0)) { // Has the bug, but supports lazy objects
            return new AggProvider(in.readMap(), null, null);
        } else { // only supports eagerly parsed objects
            // Upstream, we have read the bool already and know for sure that we have parsed aggs in the stream
            return AggProvider.fromParsedAggs(new AggregatorFactories.Builder(in));
        }
    }

    public AggProvider(Map<String, Object> aggs, AggregatorFactories.Builder parsedAggs, Exception parsingException) {
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
        if (out.getVersion().onOrAfter(Version.V_6_7_0)) { // Has our bug fix for query/agg providers
            out.writeMap(aggs);
            out.writeOptionalWriteable(parsedAggs);
            out.writeException(parsingException);
        } else if (out.getVersion().onOrAfter(Version.V_6_6_0)) { // Has the bug, but supports lazy objects
            // We allow the lazy parsing nodes that have the bug throw any parsing errors themselves as
            // they already have the ability to fully parse the passed Maps
            out.writeMap(aggs);
        } else { // only supports eagerly parsed objects
            if (parsingException != null) {
                if (parsingException instanceof IOException) {
                    throw (IOException) parsingException;
                } else {
                    throw new ElasticsearchException(parsingException);
                }
            } else if (parsedAggs == null) {
                // This is an admittedly rare case but we should fail early instead of writing null when there
                // actually are aggregations defined
                throw new ElasticsearchException("Unsupported operation: parsed aggregations are null");
            }
            // Upstream we already verified that this calling object is not null, no need to write a second boolean to the stream
            parsedAggs.writeTo(out);
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
