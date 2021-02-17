/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.WriteableZoneId;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Metadata about rollup indices that includes information about their capabilities,
 * such as which date-intervals and date-timezones they are configured with and what metric aggregations
 * do they support.
 *
 * The information in this class will be used to decide which index within a rollup group will be chosen
 * for a specific aggregation. For example, if there are two indices with different intervals (`1h`, `1d`) and
 * a date-histogram aggregation request is sent for daily intervals, then the index with the associated `1d` interval
 * will be chosen.
 */
public class RollupIndexMetadata extends AbstractDiffable<RollupIndexMetadata> implements ToXContentObject {

    public static final String TYPE = "rollup";
    public static final String SOURCE_INDEX_NAME_META_FIELD = "source_index";
    public static final String ROLLUP_META_FIELD = "meta";

    private static final ParseField DATE_INTERVAL_FIELD = new ParseField("interval");
    private static final ParseField DATE_TIMEZONE_FIELD = new ParseField("timezone");
    private static final ParseField METRICS_FIELD = new ParseField("metrics");

    // the date interval used for rolling up data
    private final DateHistogramInterval dateInterval;
    // the timezone used for the date_histogram
    private final WriteableZoneId dateTimezone;
    // a map from field name to metrics supported by the rollup for this field
    private final Map<String, List<String>> metrics;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RollupIndexMetadata, Void> PARSER =
        new ConstructingObjectParser<>(ROLLUP_META_FIELD, false,
            a -> new RollupIndexMetadata((DateHistogramInterval) a[0], (WriteableZoneId) a[1], (Map<String, List<String>>) a[2]));

    static {
        PARSER.declareField(optionalConstructorArg(),
            p -> new DateHistogramInterval(p.text()), DATE_INTERVAL_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(),
            p -> WriteableZoneId.of(p.text()), DATE_TIMEZONE_FIELD, ObjectParser.ValueType.STRING);

        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, List<String>> metricsMap = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = p.currentName();
                p.nextToken();
                List<Object> metricObj = p.list();
                List<String> metrics = new ArrayList<>(metricObj.size());
                for (Object o:  metricObj) {
                    metrics.add((String) o);
                }
                metricsMap.put(fieldName, metrics);
            }
            return metricsMap;
        }, METRICS_FIELD);
    }

    public RollupIndexMetadata(DateHistogramInterval dateInterval,
                               WriteableZoneId dateTimezone,
                               Map<String, List<String>> metrics) {
        this.dateInterval = dateInterval;
        this.dateTimezone = dateTimezone;
        this.metrics = metrics;
    }

    public RollupIndexMetadata(StreamInput in) throws IOException {
        this.dateInterval = new DateHistogramInterval(in);
        this.dateTimezone = new WriteableZoneId(in);
        this.metrics = in.readMapOfLists(StreamInput::readString, StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        dateInterval.writeTo(out);
        dateTimezone.writeTo(out);
        out.writeMapOfLists(metrics, StreamOutput::writeString, StreamOutput::writeString);
    }

    public static RollupIndexMetadata parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    static RollupIndexMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static RollupIndexMetadata parseMetadataXContent(String content) throws IOException{
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE, content);
        return PARSER.apply(parser, null);
    }

    public DateHistogramInterval getDateInterval() {
        return dateInterval;
    }

    public WriteableZoneId getDateTimezone() {
        return dateTimezone;
    }

    public Map<String, List<String>> getMetrics() {
        return metrics;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(DATE_INTERVAL_FIELD.getPreferredName(), dateInterval)
            .field(DATE_TIMEZONE_FIELD.getPreferredName(), dateTimezone)
            .field(METRICS_FIELD.getPreferredName(), metrics)
            .endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RollupIndexMetadata that = (RollupIndexMetadata) o;
        return dateInterval.equals(that.dateInterval) &&
            dateTimezone.equals(that.dateTimezone) &&
            Objects.equals(this.metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateInterval, dateTimezone, metrics);
    }

}
