/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_DATASET;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_TYPE;

/**
 * This class represents Analytics events object meant to be emitted to the event queue.
 */
public class AnalyticsEvent implements Writeable, ToXContentObject {

    public static final ParseField TIMESTAMP_FIELD = new ParseField("@timestamp");
    public static final ParseField EVENT_FIELD = new ParseField("event");
    public static final ParseField EVENT_ACTION_FIELD = new ParseField("action");
    public static final ParseField DATA_STREAM_FIELD = new ParseField("data_stream");
    public static final ParseField DATA_STREAM_TYPE_FIELD = new ParseField("type");
    public static final ParseField DATA_STREAM_NAMESPACE_FIELD = new ParseField("namespace");
    public static final ParseField DATA_STREAM_DATASET_FIELD = new ParseField("dataset");
    public static final ParseField CLIENT_ADDRESS_FIELD = new ParseField("client_address");
    public static final ParseField USER_AGENT_FIELD = new ParseField("user_agent");
    private final String userAgent;

    /**
     * Analytics event types.
     */
    public enum Type {
        PAGE_VIEW("page_view"),
        SEARCH("search"),
        SEARCH_CLICK("search_click");

        private final String typeName;

        Type(String typeName) {
            this.typeName = typeName;
        }

        @Override
        public String toString() {
            return typeName.toLowerCase(Locale.ROOT);
        }
    }

    private final String eventCollectionName;

    private final Type eventType;

    private final long eventTime;

    private final BytesReference payload;

    private final XContentType xContentType;
    private final InetAddress clientAddress;

    protected AnalyticsEvent(
        String eventCollectionName,
        long eventTime,
        Type eventType,
        XContentType xContentType,
        BytesReference payload,
        @Nullable String userAgent,
        @Nullable InetAddress clientAddress
    ) {
        this.eventCollectionName = Strings.requireNonBlank(eventCollectionName, "eventCollectionName cannot be null");
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.xContentType = Objects.requireNonNull(xContentType, "xContentType cannot be null");
        this.payload = Objects.requireNonNull(payload, "payload cannot be null");
        this.userAgent = userAgent;
        this.clientAddress = clientAddress;
    }

    public AnalyticsEvent(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readLong(),
            in.readEnum(Type.class),
            in.readEnum(XContentType.class),
            in.readBytesReference(),
            in.readOptionalString(),
            readInetAddress(in)
        );
    }

    private static InetAddress readInetAddress(StreamInput in) {
        try {
            return InetAddresses.forString(in.readOptionalString());
        } catch (Exception e) {
            return null;
        }
    }

    public String userAgent() {
        return userAgent;
    }

    public static Builder builder(AnalyticsEvent.Context context) {
        return new AnalyticsEvent.Builder(context);
    }

    public String eventCollectionName() {
        return eventCollectionName;
    }

    public long eventTime() {
        return eventTime;
    }

    public Type eventType() {
        return eventType;
    }

    public XContentType xContentType() {
        return xContentType;
    }

    public BytesReference payload() {
        return payload;
    }

    public Map<String, Object> payloadAsMap() {
        return XContentHelper.convertToMap(payload(), true, xContentType()).v2();
    }

    public InetAddress clientAddress() {
        return clientAddress;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(eventCollectionName);
        out.writeLong(eventTime);
        out.writeEnum(eventType);
        XContentHelper.writeTo(out, xContentType);
        out.writeBytesReference(payload);
        out.writeOptionalString(userAgent);
        out.writeOptionalString(clientAddress == null ? null : NetworkAddress.format(clientAddress));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(TIMESTAMP_FIELD.getPreferredName(), eventTime());

            builder.startObject(EVENT_FIELD.getPreferredName());
            {
                builder.field(EVENT_ACTION_FIELD.getPreferredName(), eventType());
            }
            builder.endObject();

            builder.startObject(DATA_STREAM_FIELD.getPreferredName());
            {
                builder.field(DATA_STREAM_TYPE_FIELD.getPreferredName(), EVENT_DATA_STREAM_TYPE);
                builder.field(DATA_STREAM_DATASET_FIELD.getPreferredName(), EVENT_DATA_STREAM_DATASET);
                builder.field(DATA_STREAM_NAMESPACE_FIELD.getPreferredName(), eventCollectionName());

            }
            builder.endObject();

            builder.mapContents(payloadAsMap());

            if (userAgent != null) {
                builder.field(USER_AGENT_FIELD.getPreferredName(), userAgent);
            }

            if (clientAddress != null) {
                builder.field(CLIENT_ADDRESS_FIELD.getPreferredName(), NetworkAddress.format(clientAddress));
            }
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEvent that = (AnalyticsEvent) o;
        return eventCollectionName.equals(that.eventCollectionName)
            && eventTime == that.eventTime
            && eventType == that.eventType
            && xContentType.equals(that.xContentType)
            && payloadAsMap().equals(that.payloadAsMap())
            && Objects.equals(clientAddress, that.clientAddress)
            && Objects.equals(userAgent, that.userAgent);
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventCollectionName, eventTime, xContentType, payloadAsMap(), clientAddress, userAgent);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Analytics context. Used to carry information to parsers.
     */
    public interface Context {
        long eventTime();

        Type eventType();

        String eventCollectionName();

        String userAgent();

        InetAddress clientAddress();

        default AnalyticsCollection analyticsCollection() {
            // TODO: remove. Only used in tests.
            return new AnalyticsCollection(eventCollectionName());
        }

        // TODO: Move the interface to the package (renamed into AnalyticsContext)
    }

    public static class Builder {
        private final MapBuilder<String, Object> payloadBuilder = new MapBuilder<>();

        private final Context context;

        private Builder(Context context) {
            this.context = context;
        }

        public AnalyticsEvent build() throws IOException {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                BytesReference payload = BytesReference.bytes(builder.map(payloadBuilder.map()));
                return new AnalyticsEvent(
                    context.eventCollectionName(),
                    context.eventTime(),
                    context.eventType(),
                    builder.contentType(),
                    payload,
                    context.userAgent(),
                    context.clientAddress()
                );
            }
        }

        public Builder withField(String fieldName, Object fieldValue) {
            if (Objects.nonNull(fieldValue)) {
                payloadBuilder.put(fieldName, fieldValue);
            }

            return this;
        }

        public Builder withField(ParseField field, Object fieldValue) {
            return this.withField(field.getPreferredName(), fieldValue);
        }

        public Builder with(Map<String, Object> values) {
            payloadBuilder.putAll(values);
            return this;
        }
    }
}
