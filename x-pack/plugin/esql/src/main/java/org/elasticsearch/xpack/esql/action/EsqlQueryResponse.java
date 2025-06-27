/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.TransportVersions.ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED;
import static org.elasticsearch.TransportVersions.ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED_8_19;

public class EsqlQueryResponse extends org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse
    implements
        ChunkedToXContentObject,
        Releasable {

    @SuppressWarnings("this-escape")
    private final AbstractRefCounted counted = AbstractRefCounted.of(this::closeInternal);

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
        EsqlExecutionInfo executionInfo
    ) {
        this(columns, pages, documentsFound, valuesLoaded, profile, columnar, null, false, isAsync, executionInfo);
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
        String asyncExecutionId = null;
        boolean isRunning = false;
        boolean isAsync = false;
        Profile profile = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            asyncExecutionId = in.readOptionalString();
            isRunning = in.readBoolean();
            isAsync = in.readBoolean();
        }
        List<ColumnInfoImpl> columns = in.readCollectionAsList(ColumnInfoImpl::new);
        List<Page> pages = in.readCollectionAsList(Page::new);
        long documentsFound = supportsValuesLoaded(in.getTransportVersion()) ? in.readVLong() : 0;
        long valuesLoaded = supportsValuesLoaded(in.getTransportVersion()) ? in.readVLong() : 0;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            profile = in.readOptionalWriteable(Profile::readFrom);
        }
        boolean columnar = in.readBoolean();
        EsqlExecutionInfo executionInfo = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            executionInfo = in.readOptionalWriteable(EsqlExecutionInfo::new);
        }
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
            executionInfo
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeOptionalString(asyncExecutionId);
            out.writeBoolean(isRunning);
            out.writeBoolean(isAsync);
        }
        out.writeCollection(columns);
        out.writeCollection(pages);
        if (supportsValuesLoaded(out.getTransportVersion())) {
            out.writeVLong(documentsFound);
            out.writeVLong(valuesLoaded);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalWriteable(profile);
        }
        out.writeBoolean(columnar);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeOptionalWriteable(executionInfo);
        }
    }

    private static boolean supportsValuesLoaded(TransportVersion version) {
        return version.onOrAfter(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED)
            || version.isPatchFrom(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED_8_19);
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
        return isRunning;
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
            content.add(
                ChunkedToXContentHelper.chunk(
                    (builder, p) -> builder //
                        .field("took", executionInfo.overallTook().millis())
                        .field(EsqlExecutionInfo.IS_PARTIAL_FIELD.getPreferredName(), executionInfo.isPartial())
                )
            );
        }
        content.add(
            ChunkedToXContentHelper.chunk(
                (builder, p) -> builder //
                    .field("documents_found", documentsFound)
                    .field("values_loaded", valuesLoaded)
            )
        );
        if (dropNullColumns) {
            content.add(ResponseXContentUtils.allColumns(columns, "all_columns"));
            content.add(ResponseXContentUtils.nonNullColumns(columns, nullColumns, "columns"));
        } else {
            content.add(ResponseXContentUtils.allColumns(columns, "columns"));
        }
        content.add(
            ChunkedToXContentHelper.array("values", ResponseXContentUtils.columnValues(this.columns, this.pages, columnar, nullColumns))
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

    public record Profile(List<DriverProfile> drivers, List<PlanProfile> plans) implements Writeable {

        public static Profile readFrom(StreamInput in) throws IOException {
            return new Profile(
                in.readCollectionAsImmutableList(DriverProfile::readFrom),
                in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_INCLUDE_PLAN)
                    ? in.readCollectionAsImmutableList(PlanProfile::readFrom)
                    : List.of()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(drivers);
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_INCLUDE_PLAN)) {
                out.writeCollection(plans);
            }
        }
    }
}
