/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver.ESQL_USE_MINIMUM_VERSION_FOR_ENRICH_RESOLUTION;

public class EsqlQueryResponse extends org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse
    implements
        ChunkedToXContentObject,
        Releasable {

    @SuppressWarnings("this-escape")
    private final AbstractRefCounted counted = AbstractRefCounted.of(this::closeInternal);

    private static final TransportVersion ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED = TransportVersion.fromName(
        "esql_documents_found_and_values_loaded"
    );
    private static final TransportVersion ESQL_PROFILE_INCLUDE_PLAN = TransportVersion.fromName("esql_profile_include_plan");
    private static final TransportVersion ESQL_TIMESTAMPS_INFO = TransportVersion.fromName("esql_timestamps_info");
    private static final TransportVersion ESQL_RESPONSE_TIMEZONE_FORMAT = TransportVersion.fromName("esql_response_timezone_format");

    public static final String DROP_NULL_COLUMNS_OPTION = "drop_null_columns";

    private final List<ColumnInfoImpl> columns;
    private final List<Page> pages;
    private final long documentsFound;
    private final long valuesLoaded;
    private final Profile profile;
    private final boolean columnar;
    private final String asyncExecutionId;
    private final boolean isRunning;
    // True if this response is as a result of an async query request
    private final boolean isAsync;
    private final EsqlExecutionInfo executionInfo;

    private final long startTimeMillis;
    private final long expirationTimeMillis;

    private final ZoneId zoneId;

    public EsqlQueryResponse(
        List<ColumnInfoImpl> columns,
        List<Page> pages,
        long documentsFound,
        long valuesLoaded,
        @Nullable Profile profile,
        boolean columnar,
        @Nullable String asyncExecutionId,
        boolean isRunning,
        boolean isAsync,
        ZoneId zoneId,
        long startTimeMillis,
        long expirationTimeMillis,
        EsqlExecutionInfo executionInfo
    ) {
        this.columns = columns;
        this.pages = pages;
        this.valuesLoaded = valuesLoaded;
        this.documentsFound = documentsFound;
        this.profile = profile;
        this.columnar = columnar;
        this.asyncExecutionId = asyncExecutionId;
        this.isRunning = isRunning;
        this.isAsync = isAsync;
        this.zoneId = zoneId;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.executionInfo = executionInfo;
    }

    public EsqlQueryResponse(
        List<ColumnInfoImpl> columns,
        List<Page> pages,
        long documentsFound,
        long valuesLoaded,
        @Nullable Profile profile,
        boolean columnar,
        boolean isAsync,
        ZoneId zoneId,
        long startTimeMillis,
        long expirationTimeMillis,
        EsqlExecutionInfo executionInfo
    ) {
        this(
            columns,
            pages,
            documentsFound,
            valuesLoaded,
            profile,
            columnar,
            null,
            false,
            isAsync,
            zoneId,
            startTimeMillis,
            expirationTimeMillis,
            executionInfo
        );
    }

    /**
     * Build a reader for the response.
     */
    public static Writeable.Reader<EsqlQueryResponse> reader(BlockFactory blockFactory) {
        return in -> {
            try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                return deserialize(bsi);
            }
        };
    }

    static EsqlQueryResponse deserialize(BlockStreamInput in) throws IOException {
        String asyncExecutionId = in.readOptionalString();
        boolean isRunning = in.readBoolean();
        boolean isAsync = in.readBoolean();
        List<ColumnInfoImpl> columns = in.readCollectionAsList(ColumnInfoImpl::new);
        List<Page> pages = in.readCollectionAsList(Page::new);
        long documentsFound = supportsValuesLoaded(in.getTransportVersion()) ? in.readVLong() : 0;
        long valuesLoaded = supportsValuesLoaded(in.getTransportVersion()) ? in.readVLong() : 0;
        Profile profile = in.readOptionalWriteable(Profile::readFrom);
        boolean columnar = in.readBoolean();

        long startTimeMillis = 0L;
        long expirationTimeMillis = 0L;
        if (in.getTransportVersion().supports(ESQL_TIMESTAMPS_INFO)) {
            startTimeMillis = in.readLong();
            expirationTimeMillis = in.readLong();
        }

        ZoneId zoneId = ZoneOffset.UTC;
        if (in.getTransportVersion().supports(ESQL_RESPONSE_TIMEZONE_FORMAT)) {
            zoneId = in.readZoneId();
        }

        EsqlExecutionInfo executionInfo = in.readOptionalWriteable(EsqlExecutionInfo::new);
        return new EsqlQueryResponse(
            columns,
            pages,
            documentsFound,
            valuesLoaded,
            profile,
            columnar,
            asyncExecutionId,
            isRunning,
            isAsync,
            zoneId,
            startTimeMillis,
            expirationTimeMillis,
            executionInfo
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(asyncExecutionId);
        out.writeBoolean(isRunning);
        out.writeBoolean(isAsync);
        out.writeCollection(columns);
        out.writeCollection(pages);
        if (supportsValuesLoaded(out.getTransportVersion())) {
            out.writeVLong(documentsFound);
            out.writeVLong(valuesLoaded);
        }
        out.writeOptionalWriteable(profile);
        out.writeBoolean(columnar);

        if (out.getTransportVersion().supports(ESQL_TIMESTAMPS_INFO)) {
            out.writeLong(startTimeMillis);
            out.writeLong(expirationTimeMillis);
        }

        if (out.getTransportVersion().supports(ESQL_RESPONSE_TIMEZONE_FORMAT)) {
            out.writeZoneId(zoneId);
        }

        out.writeOptionalWriteable(executionInfo);
    }

    private static boolean supportsValuesLoaded(TransportVersion version) {
        return version.supports(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED);
    }

    public List<ColumnInfoImpl> columns() {
        return columns;
    }

    List<Page> pages() {
        return pages;
    }

    public Iterator<Iterator<Object>> values() {
        List<DataType> dataTypes = columns.stream().map(ColumnInfoImpl::type).toList();
        return ResponseValueUtils.pagesToValues(dataTypes, pages);
    }

    public Iterable<Iterable<Object>> rows() {
        List<DataType> dataTypes = columns.stream().map(ColumnInfoImpl::type).toList();
        return ResponseValueUtils.valuesForRowsInPages(dataTypes, pages);
    }

    public Iterator<Object> column(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columns.size()) throw new IllegalArgumentException();
        return ResponseValueUtils.valuesForColumn(columnIndex, columns.get(columnIndex).type(), pages);
    }

    /**
     * @return the number of "documents" we got back from lucene, as input into the compute engine. Note that in this context, we think
     * of things like the result of LuceneMaxOperator as single documents.
     */
    public long documentsFound() {
        return documentsFound;
    }

    public long valuesLoaded() {
        return valuesLoaded;
    }

    public Profile profile() {
        return profile;
    }

    public boolean columnar() {
        return columnar;
    }

    public Optional<String> asyncExecutionId() {
        return Optional.ofNullable(asyncExecutionId);
    }

    public boolean isRunning() {
        return isRunning;
    }

    public boolean isAsync() {
        return isAsync;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    public boolean isPartial() {
        return executionInfo != null && executionInfo.isPartial();
    }

    public EsqlExecutionInfo getExecutionInfo() {
        return executionInfo;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        boolean dropNullColumns = params.paramAsBoolean(DROP_NULL_COLUMNS_OPTION, false);
        boolean[] nullColumns = dropNullColumns ? nullColumns() : null;

        var content = new ArrayList<Iterator<? extends ToXContent>>(25);
        content.add(ChunkedToXContentHelper.startObject());
        if (isAsync) {
            content.add(ChunkedToXContentHelper.chunk((builder, p) -> {
                if (asyncExecutionId != null) {
                    builder.field("id", asyncExecutionId);
                }
                builder.field("is_running", isRunning);
                return builder;
            }));
        }
        if (executionInfo != null && executionInfo.overallTook() != null) {
            content.add(ChunkedToXContentHelper.chunk((builder, p) -> {
                builder //
                    .field("took", executionInfo.overallTook().millis())
                    .field(EsqlExecutionInfo.IS_PARTIAL_FIELD.getPreferredName(), executionInfo.isPartial());

                if (startTimeMillis != 0L) {
                    builder.timestampFieldsFromUnixEpochMillis(
                        "completion_time_in_millis",
                        "completion_time",
                        startTimeMillis + executionInfo.overallTook().millis()
                    );
                }

                return builder;
            }));
        }
        content.add(ChunkedToXContentHelper.chunk((builder, p) -> {
            builder //
                .field("documents_found", documentsFound)
                .field("values_loaded", valuesLoaded);

            if (startTimeMillis != 0L) {
                builder.timestampFieldsFromUnixEpochMillis("start_time_in_millis", "start_time", startTimeMillis);
            }
            if (expirationTimeMillis != 0L) {
                builder.timestampFieldsFromUnixEpochMillis("expiration_time_in_millis", "expiration_time", expirationTimeMillis);
            }

            return builder;
        }));
        if (dropNullColumns) {
            content.add(ResponseXContentUtils.allColumns(columns, "all_columns"));
            content.add(ResponseXContentUtils.nonNullColumns(columns, nullColumns, "columns"));
        } else {
            content.add(ResponseXContentUtils.allColumns(columns, "columns"));
        }
        content.add(
            ChunkedToXContentHelper.array(
                "values",
                ResponseXContentUtils.columnValues(this.columns, this.pages, columnar, nullColumns, zoneId)
            )
        );
        if (executionInfo != null && executionInfo.hasMetadataToReport()) {
            content.add(ChunkedToXContentHelper.field("_clusters", executionInfo, params));
        }
        if (profile != null) {
            content.add(ChunkedToXContentHelper.startObject("profile"));
            content.add(ChunkedToXContentHelper.chunk((b, p) -> {
                if (executionInfo != null) {
                    b.field("query", executionInfo.overallTimeSpan());
                    b.field("planning", executionInfo.planningTimeSpan());
                }
                return b;
            }));
            content.add(ChunkedToXContentHelper.array("drivers", profile.drivers.iterator(), params));
            content.add(ChunkedToXContentHelper.array("plans", profile.plans.iterator()));
            content.add(ChunkedToXContentHelper.chunk((b, p) -> {
                TransportVersion minimumVersion = profile.minimumVersion();
                b.field("minimumTransportVersion", minimumVersion == null ? null : minimumVersion.id());
                return b;
            }));
            content.add(ChunkedToXContentHelper.endObject());
        }
        content.add(ChunkedToXContentHelper.endObject());

        return Iterators.concat(content.toArray(Iterator[]::new));
    }

    public boolean[] nullColumns() {
        boolean[] nullColumns = new boolean[columns.size()];
        for (int c = 0; c < nullColumns.length; c++) {
            nullColumns[c] = allColumnsAreNull(c);
        }
        return nullColumns;
    }

    private boolean allColumnsAreNull(int c) {
        for (Page page : pages) {
            if (page.getBlock(c).areAllValuesNull() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EsqlQueryResponse that = (EsqlQueryResponse) o;
        return Objects.equals(columns, that.columns)
            && Objects.equals(asyncExecutionId, that.asyncExecutionId)
            && Objects.equals(isRunning, that.isRunning)
            && columnar == that.columnar
            && Iterators.equals(values(), that.values(), (row1, row2) -> Iterators.equals(row1, row2, Objects::equals))
            && documentsFound == that.documentsFound
            && valuesLoaded == that.valuesLoaded
            && Objects.equals(profile, that.profile)
            && Objects.equals(executionInfo, that.executionInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            asyncExecutionId,
            isRunning,
            columns,
            columnar,
            Iterators.hashCode(values(), row -> Iterators.hashCode(row, Objects::hashCode)),
            documentsFound,
            valuesLoaded,
            profile,
            executionInfo
        );
    }

    @Override
    public String toString() {
        return Strings.toString(ChunkedToXContent.wrapAsToXContent(this));
    }

    @Override
    public void incRef() {
        tryIncRef();
    }

    @Override
    public boolean tryIncRef() {
        return counted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return counted.decRef();
    }

    @Override
    public boolean hasReferences() {
        return counted.hasReferences();
    }

    @Override
    public void close() {
        super.close();
        decRef();
        if (esqlResponse != null) {
            esqlResponse.setClosedState();
        }
    }

    void closeInternal() {
        Releasables.close(() -> Iterators.map(pages.iterator(), p -> p::releaseBlocks));
    }

    // singleton lazy set view over this response
    private EsqlResponseImpl esqlResponse;

    @Override
    public EsqlResponse responseInternal() {
        if (hasReferences() == false) {
            throw new IllegalStateException("closed");
        }
        if (esqlResponse != null) {
            return esqlResponse;
        }
        esqlResponse = new EsqlResponseImpl(this);
        return esqlResponse;
    }

    public record Profile(List<DriverProfile> drivers, List<PlanProfile> plans, TransportVersion minimumVersion) implements Writeable {

        public static Profile readFrom(StreamInput in) throws IOException {
            return new Profile(
                in.readCollectionAsImmutableList(DriverProfile::readFrom),
                in.getTransportVersion().supports(ESQL_PROFILE_INCLUDE_PLAN)
                    ? in.readCollectionAsImmutableList(PlanProfile::readFrom)
                    : List.of(),
                in.getTransportVersion().supports(ESQL_USE_MINIMUM_VERSION_FOR_ENRICH_RESOLUTION) ? readOptionalTransportVersion(in) : null
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(drivers);
            if (out.getTransportVersion().supports(ESQL_PROFILE_INCLUDE_PLAN)) {
                out.writeCollection(plans);
            }
            if (out.getTransportVersion().supports(ESQL_USE_MINIMUM_VERSION_FOR_ENRICH_RESOLUTION)) {
                // When retrieving the profile from an older node, there might be no minimum version attached.
                // When writing the profile somewhere else, we need to handle the case that the minimum version is null.
                writeOptionalTransportVersion(minimumVersion, out);
            }
        }

        private static TransportVersion readOptionalTransportVersion(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                return TransportVersion.readVersion(in);
            }
            return null;
        }

        private static void writeOptionalTransportVersion(@Nullable TransportVersion version, StreamOutput out) throws IOException {
            if (version == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                TransportVersion.writeVersion(version, out);
            }
        }
    }
}
