/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.datastreams;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAutoShardingEvent;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.V_8_11_X;
import static org.elasticsearch.cluster.metadata.DataStream.AUTO_SHARDING_FIELD;

public class GetDataStreamAction extends ActionType<GetDataStreamAction.Response> {

    public static final GetDataStreamAction INSTANCE = new GetDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/get";

    private GetDataStreamAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeReadRequest<Request> implements IndicesRequest.Replaceable {

        private String[] names;
        private IndicesOptions indicesOptions = IndicesOptions.builder()
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
            .wildcardOptions(
                IndicesOptions.WildcardOptions.builder()
                    .matchOpen(true)
                    .matchClosed(true)
                    .includeHidden(false)
                    .resolveAliases(false)
                    .allowEmptyExpressions(true)
                    .build()
            )
            .gatekeeperOptions(
                IndicesOptions.GatekeeperOptions.builder()
                    .allowAliasToMultipleIndices(false)
                    .allowClosedIndices(true)
                    .ignoreThrottled(false)
                    .allowFailureIndices(true)
                    .build()
            )
            .build();
        private boolean includeDefaults = false;
        private boolean verbose = false;

        public Request(TimeValue masterNodeTimeout, String[] names) {
            super(masterNodeTimeout);
            this.names = names;
        }

        public Request(TimeValue masterNodeTimeout, String[] names, boolean includeDefaults) {
            super(masterNodeTimeout);
            this.names = names;
            this.includeDefaults = includeDefaults;
        }

        public String[] getNames() {
            return names;
        }

        public boolean verbose() {
            return verbose;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readOptionalStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                this.includeDefaults = in.readBoolean();
            } else {
                this.includeDefaults = false;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                this.verbose = in.readBoolean();
            } else {
                this.verbose = false;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(names);
            indicesOptions.writeIndicesOptions(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                out.writeBoolean(includeDefaults);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                out.writeBoolean(verbose);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names)
                && indicesOptions.equals(request.indicesOptions)
                && includeDefaults == request.includeDefaults
                && verbose == request.verbose;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(indicesOptions, includeDefaults, verbose);
            result = 31 * result + Arrays.hashCode(names);
            return result;
        }

        @Override
        public String[] indices() {
            return names;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public boolean includeDefaults() {
            return includeDefaults;
        }

        public Request indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.names = indices;
            return this;
        }

        public Request includeDefaults(boolean includeDefaults) {
            this.includeDefaults = includeDefaults;
            return this;
        }

        public Request verbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public enum ManagedBy {
            ILM("Index Lifecycle Management"),
            LIFECYCLE("Data stream lifecycle"),
            UNMANAGED("Unmanaged");

            public final String displayValue;

            ManagedBy(String displayValue) {
                this.displayValue = displayValue;
            }
        }

        public static final ParseField DATA_STREAMS_FIELD = new ParseField("data_streams");

        public static class DataStreamInfo implements SimpleDiffable<DataStreamInfo>, ToXContentObject {

            public static final ParseField STATUS_FIELD = new ParseField("status");
            public static final ParseField INDEX_TEMPLATE_FIELD = new ParseField("template");
            public static final ParseField PREFER_ILM = new ParseField("prefer_ilm");
            public static final ParseField MANAGED_BY = new ParseField("managed_by");
            public static final ParseField NEXT_GENERATION_INDEX_MANAGED_BY = new ParseField("next_generation_managed_by");
            public static final ParseField ILM_POLICY_FIELD = new ParseField("ilm_policy");
            public static final ParseField LIFECYCLE_FIELD = new ParseField("lifecycle");
            public static final ParseField HIDDEN_FIELD = new ParseField("hidden");
            public static final ParseField SYSTEM_FIELD = new ParseField("system");
            public static final ParseField ALLOW_CUSTOM_ROUTING = new ParseField("allow_custom_routing");
            public static final ParseField REPLICATED = new ParseField("replicated");
            public static final ParseField ROLLOVER_ON_WRITE = new ParseField("rollover_on_write");
            public static final ParseField TIME_SERIES = new ParseField("time_series");
            public static final ParseField TEMPORAL_RANGES = new ParseField("temporal_ranges");
            public static final ParseField TEMPORAL_RANGE_START = new ParseField("start");
            public static final ParseField TEMPORAL_RANGE_END = new ParseField("end");
            public static final ParseField TIME_SINCE_LAST_AUTO_SHARD_EVENT = new ParseField("time_since_last_auto_shard_event");
            public static final ParseField TIME_SINCE_LAST_AUTO_SHARD_EVENT_MILLIS = new ParseField(
                "time_since_last_auto_shard_event_millis"
            );
            public static final ParseField FAILURE_STORE_ENABLED = new ParseField("enabled");
            public static final ParseField MAXIMUM_TIMESTAMP = new ParseField("maximum_timestamp");

            private final DataStream dataStream;
            private final ClusterHealthStatus dataStreamStatus;
            private final boolean failureStoreEffectivelyEnabled; // Must be serialized independently of dataStream as depends on settings
            @Nullable
            private final String indexTemplate;
            @Nullable
            private final String ilmPolicyName;
            @Nullable
            private final TimeSeries timeSeries;
            private final Map<Index, IndexProperties> indexSettingsValues;
            private final boolean templatePreferIlmValue;
            @Nullable
            private final Long maximumTimestamp;

            public DataStreamInfo(
                DataStream dataStream,
                boolean failureStoreEffectivelyEnabled,
                ClusterHealthStatus dataStreamStatus,
                @Nullable String indexTemplate,
                @Nullable String ilmPolicyName,
                @Nullable TimeSeries timeSeries,
                Map<Index, IndexProperties> indexSettingsValues,
                boolean templatePreferIlmValue,
                @Nullable Long maximumTimestamp
            ) {
                this.dataStream = dataStream;
                this.failureStoreEffectivelyEnabled = failureStoreEffectivelyEnabled;
                this.dataStreamStatus = dataStreamStatus;
                this.indexTemplate = indexTemplate;
                this.ilmPolicyName = ilmPolicyName;
                this.timeSeries = timeSeries;
                this.indexSettingsValues = indexSettingsValues;
                this.templatePreferIlmValue = templatePreferIlmValue;
                this.maximumTimestamp = maximumTimestamp;
            }

            @SuppressWarnings("unchecked")
            DataStreamInfo(StreamInput in) throws IOException {
                this.dataStream = DataStream.read(in);
                this.failureStoreEffectivelyEnabled = in.getTransportVersion()
                    .onOrAfter(TransportVersions.FAILURE_STORE_ENABLED_BY_CLUSTER_SETTING)
                        ? in.readBoolean()
                        : dataStream.isFailureStoreExplicitlyEnabled(); // Revert to the behaviour before this field was added
                this.dataStreamStatus = ClusterHealthStatus.readFrom(in);
                this.indexTemplate = in.readOptionalString();
                this.ilmPolicyName = in.readOptionalString();
                this.timeSeries = in.getTransportVersion().onOrAfter(TransportVersions.V_8_3_0)
                    ? in.readOptionalWriteable(TimeSeries::new)
                    : null;
                this.indexSettingsValues = in.getTransportVersion().onOrAfter(V_8_11_X)
                    ? in.readMap(Index::new, IndexProperties::new)
                    : Map.of();
                this.templatePreferIlmValue = in.getTransportVersion().onOrAfter(V_8_11_X) ? in.readBoolean() : true;
                this.maximumTimestamp = in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readOptionalVLong() : null;
            }

            public DataStream getDataStream() {
                return dataStream;
            }

            public boolean isFailureStoreEffectivelyEnabled() {
                return failureStoreEffectivelyEnabled;
            }

            public ClusterHealthStatus getDataStreamStatus() {
                return dataStreamStatus;
            }

            @Nullable
            public String getIndexTemplate() {
                return indexTemplate;
            }

            @Nullable
            public String getIlmPolicy() {
                return ilmPolicyName;
            }

            @Nullable
            public TimeSeries getTimeSeries() {
                return timeSeries;
            }

            public Map<Index, IndexProperties> getIndexSettingsValues() {
                return indexSettingsValues;
            }

            public boolean templatePreferIlmValue() {
                return templatePreferIlmValue;
            }

            @Nullable
            public Long getMaximumTimestamp() {
                return maximumTimestamp;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                dataStream.writeTo(out);
                if (out.getTransportVersion().onOrAfter(TransportVersions.FAILURE_STORE_ENABLED_BY_CLUSTER_SETTING)) {
                    out.writeBoolean(failureStoreEffectivelyEnabled);
                }
                dataStreamStatus.writeTo(out);
                out.writeOptionalString(indexTemplate);
                out.writeOptionalString(ilmPolicyName);
                if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_3_0)) {
                    out.writeOptionalWriteable(timeSeries);
                }
                if (out.getTransportVersion().onOrAfter(V_8_11_X)) {
                    out.writeMap(indexSettingsValues);
                    out.writeBoolean(templatePreferIlmValue);
                }
                if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                    out.writeOptionalVLong(maximumTimestamp);
                }
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return toXContent(builder, params, null, null);
            }

            /**
             * Converts the response to XContent and passes the RolloverConditions and the global retention, when provided,
             * to the data stream.
             */
            public XContentBuilder toXContent(
                XContentBuilder builder,
                Params params,
                @Nullable RolloverConfiguration rolloverConfiguration,
                @Nullable DataStreamGlobalRetention globalRetention
            ) throws IOException {
                builder.startObject();
                builder.field(DataStream.NAME_FIELD.getPreferredName(), dataStream.getName());
                builder.field(DataStream.TIMESTAMP_FIELD_FIELD.getPreferredName())
                    .startObject()
                    .field(DataStream.NAME_FIELD.getPreferredName(), DataStream.TIMESTAMP_FIELD_NAME)
                    .endObject();

                indicesToXContent(builder, dataStream.getIndices());
                builder.field(DataStream.GENERATION_FIELD.getPreferredName(), dataStream.getGeneration());
                if (dataStream.getMetadata() != null) {
                    builder.field(DataStream.METADATA_FIELD.getPreferredName(), dataStream.getMetadata());
                }
                builder.field(STATUS_FIELD.getPreferredName(), dataStreamStatus);
                if (indexTemplate != null) {
                    builder.field(INDEX_TEMPLATE_FIELD.getPreferredName(), indexTemplate);
                }
                if (dataStream.getLifecycle() != null) {
                    builder.field(LIFECYCLE_FIELD.getPreferredName());
                    dataStream.getLifecycle().toXContent(builder, params, rolloverConfiguration, globalRetention, dataStream.isInternal());
                }
                if (ilmPolicyName != null) {
                    builder.field(ILM_POLICY_FIELD.getPreferredName(), ilmPolicyName);
                }
                builder.field(NEXT_GENERATION_INDEX_MANAGED_BY.getPreferredName(), getNextGenerationManagedBy().displayValue);
                builder.field(PREFER_ILM.getPreferredName(), templatePreferIlmValue);
                builder.field(HIDDEN_FIELD.getPreferredName(), dataStream.isHidden());
                builder.field(SYSTEM_FIELD.getPreferredName(), dataStream.isSystem());
                builder.field(ALLOW_CUSTOM_ROUTING.getPreferredName(), dataStream.isAllowCustomRouting());
                builder.field(REPLICATED.getPreferredName(), dataStream.isReplicated());
                builder.field(ROLLOVER_ON_WRITE.getPreferredName(), dataStream.rolloverOnWrite());
                if (this.maximumTimestamp != null) {
                    builder.field(MAXIMUM_TIMESTAMP.getPreferredName(), this.maximumTimestamp);
                }
                addAutoShardingEvent(builder, params, dataStream.getAutoShardingEvent());
                if (timeSeries != null) {
                    builder.startObject(TIME_SERIES.getPreferredName());
                    builder.startArray(TEMPORAL_RANGES.getPreferredName());
                    for (var range : timeSeries.temporalRanges()) {
                        builder.startObject();
                        Instant start = range.v1();
                        builder.field(TEMPORAL_RANGE_START.getPreferredName(), DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(start));
                        Instant end = range.v2();
                        builder.field(TEMPORAL_RANGE_END.getPreferredName(), DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format(end));
                        builder.endObject();
                    }
                    builder.endArray();
                    builder.endObject();
                }
                if (DataStream.isFailureStoreFeatureFlagEnabled()) {
                    builder.startObject(DataStream.FAILURE_STORE_FIELD.getPreferredName());
                    builder.field(FAILURE_STORE_ENABLED.getPreferredName(), failureStoreEffectivelyEnabled);
                    builder.field(
                        DataStream.ROLLOVER_ON_WRITE_FIELD.getPreferredName(),
                        dataStream.getFailureIndices().isRolloverOnWrite()
                    );
                    indicesToXContent(builder, dataStream.getFailureIndices().getIndices());
                    addAutoShardingEvent(builder, params, dataStream.getFailureIndices().getAutoShardingEvent());
                    builder.endObject();
                }
                builder.endObject();
                return builder;
            }

            private XContentBuilder indicesToXContent(XContentBuilder builder, List<Index> indices) throws IOException {
                builder.field(DataStream.INDICES_FIELD.getPreferredName());
                builder.startArray();
                for (Index index : indices) {
                    builder.startObject();
                    index.toXContentFragment(builder);
                    IndexProperties indexProperties = indexSettingsValues.get(index);
                    if (indexProperties != null) {
                        builder.field(PREFER_ILM.getPreferredName(), indexProperties.preferIlm());
                        if (indexProperties.ilmPolicyName() != null) {
                            builder.field(ILM_POLICY_FIELD.getPreferredName(), indexProperties.ilmPolicyName());
                        }
                        builder.field(MANAGED_BY.getPreferredName(), indexProperties.managedBy.displayValue);
                    }
                    builder.endObject();
                }
                builder.endArray();
                return builder;
            }

            private void addAutoShardingEvent(XContentBuilder builder, Params params, DataStreamAutoShardingEvent autoShardingEvent)
                throws IOException {
                if (autoShardingEvent == null) {
                    return;
                }
                builder.startObject(AUTO_SHARDING_FIELD.getPreferredName());
                autoShardingEvent.toXContent(builder, params);
                builder.humanReadableField(
                    TIME_SINCE_LAST_AUTO_SHARD_EVENT_MILLIS.getPreferredName(),
                    TIME_SINCE_LAST_AUTO_SHARD_EVENT.getPreferredName(),
                    autoShardingEvent.getTimeSinceLastAutoShardingEvent(System::currentTimeMillis)
                );
                builder.endObject();
            }

            /**
             * Computes and returns which system will manage the next generation for this data stream.
             */
            public ManagedBy getNextGenerationManagedBy() {
                // both ILM and DSL are configured so let's check the prefer_ilm setting to see which system takes precedence
                if (ilmPolicyName != null && dataStream.getLifecycle() != null && dataStream.getLifecycle().isEnabled()) {
                    return templatePreferIlmValue ? ManagedBy.ILM : ManagedBy.LIFECYCLE;
                }

                if (ilmPolicyName != null) {
                    return ManagedBy.ILM;
                }

                if (dataStream.getLifecycle() != null && dataStream.getLifecycle().isEnabled()) {
                    return ManagedBy.LIFECYCLE;
                }

                return ManagedBy.UNMANAGED;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                DataStreamInfo that = (DataStreamInfo) o;
                return templatePreferIlmValue == that.templatePreferIlmValue
                    && Objects.equals(dataStream, that.dataStream)
                    && failureStoreEffectivelyEnabled == that.failureStoreEffectivelyEnabled
                    && dataStreamStatus == that.dataStreamStatus
                    && Objects.equals(indexTemplate, that.indexTemplate)
                    && Objects.equals(ilmPolicyName, that.ilmPolicyName)
                    && Objects.equals(timeSeries, that.timeSeries)
                    && Objects.equals(indexSettingsValues, that.indexSettingsValues)
                    && Objects.equals(maximumTimestamp, that.maximumTimestamp);
            }

            @Override
            public int hashCode() {
                return Objects.hash(
                    dataStream,
                    dataStreamStatus,
                    failureStoreEffectivelyEnabled,
                    indexTemplate,
                    ilmPolicyName,
                    timeSeries,
                    indexSettingsValues,
                    templatePreferIlmValue,
                    maximumTimestamp
                );
            }
        }

        public record TimeSeries(List<Tuple<Instant, Instant>> temporalRanges) implements Writeable {

            TimeSeries(StreamInput in) throws IOException {
                this(in.readCollectionAsList(in1 -> new Tuple<>(in1.readInstant(), in1.readInstant())));
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeCollection(temporalRanges, (out1, value) -> {
                    out1.writeInstant(value.v1());
                    out1.writeInstant(value.v2());
                });
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                TimeSeries that = (TimeSeries) o;
                return temporalRanges.equals(that.temporalRanges);
            }

            @Override
            public int hashCode() {
                return Objects.hash(temporalRanges);
            }
        }

        /**
         * Encapsulates the configured properties we want to display for each backing index.
         * They'll usually be settings values, but could also be additional properties derived from settings.
         */
        public record IndexProperties(boolean preferIlm, @Nullable String ilmPolicyName, ManagedBy managedBy) implements Writeable {
            public IndexProperties(StreamInput in) throws IOException {
                this(in.readBoolean(), in.readOptionalString(), in.readEnum(ManagedBy.class));
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeBoolean(preferIlm);
                out.writeOptionalString(ilmPolicyName);
                out.writeEnum(managedBy);
            }
        }

        private final List<DataStreamInfo> dataStreams;
        @Nullable
        private final RolloverConfiguration rolloverConfiguration;
        @Nullable
        private final DataStreamGlobalRetention globalRetention;

        public Response(List<DataStreamInfo> dataStreams) {
            this(dataStreams, null, null);
        }

        public Response(
            List<DataStreamInfo> dataStreams,
            @Nullable RolloverConfiguration rolloverConfiguration,
            @Nullable DataStreamGlobalRetention globalRetention
        ) {
            this.dataStreams = dataStreams;
            this.rolloverConfiguration = rolloverConfiguration;
            this.globalRetention = globalRetention;
        }

        public Response(StreamInput in) throws IOException {
            this(
                in.readCollectionAsList(DataStreamInfo::new),
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X) ? in.readOptionalWriteable(RolloverConfiguration::new) : null,
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)
                    ? in.readOptionalWriteable(DataStreamGlobalRetention::read)
                    : null
            );
        }

        public List<DataStreamInfo> getDataStreams() {
            return dataStreams;
        }

        @Nullable
        public RolloverConfiguration getRolloverConfiguration() {
            return rolloverConfiguration;
        }

        @Nullable
        public DataStreamGlobalRetention getGlobalRetention() {
            return globalRetention;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(dataStreams);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                out.writeOptionalWriteable(rolloverConfiguration);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                out.writeOptionalWriteable(globalRetention);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray(DATA_STREAMS_FIELD.getPreferredName());
            for (DataStreamInfo dataStream : dataStreams) {
                dataStream.toXContent(
                    builder,
                    DataStreamLifecycle.addEffectiveRetentionParams(params),
                    rolloverConfiguration,
                    globalRetention
                );
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return dataStreams.equals(response.dataStreams)
                && Objects.equals(rolloverConfiguration, response.rolloverConfiguration)
                && Objects.equals(globalRetention, response.globalRetention);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStreams, rolloverConfiguration, globalRetention);
        }
    }

}
