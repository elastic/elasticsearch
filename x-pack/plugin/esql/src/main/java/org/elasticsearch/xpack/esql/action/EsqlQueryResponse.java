/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

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
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class EsqlQueryResponse extends org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse
    implements
        ChunkedToXContentObject,
        Releasable {

    @SuppressWarnings("this-escape")
    private final AbstractRefCounted counted = AbstractRefCounted.of(this::closeInternal);

    public static final String DROP_NULL_COLUMNS_OPTION = "drop_null_columns";

    private final List<ColumnInfo> columns;
    private final List<Page> pages;
    private final Profile profile;
    private final boolean columnar;
    private final String asyncExecutionId;
    private final boolean isRunning;
    // True if this response is as a result of an async query request
    private final boolean isAsync;

    public EsqlQueryResponse(
        List<ColumnInfo> columns,
        List<Page> pages,
        @Nullable Profile profile,
        boolean columnar,
        @Nullable String asyncExecutionId,
        boolean isRunning,
        boolean isAsync
    ) {
        this.columns = columns;
        this.pages = pages;
        this.profile = profile;
        this.columnar = columnar;
        this.asyncExecutionId = asyncExecutionId;
        this.isRunning = isRunning;
        this.isAsync = isAsync;
    }

    public EsqlQueryResponse(List<ColumnInfo> columns, List<Page> pages, @Nullable Profile profile, boolean columnar, boolean isAsync) {
        this(columns, pages, profile, columnar, null, false, isAsync);
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
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_ASYNC_QUERY)) {
            asyncExecutionId = in.readOptionalString();
            isRunning = in.readBoolean();
            isAsync = in.readBoolean();
        }
        List<ColumnInfo> columns = in.readCollectionAsList(ColumnInfo::new);
        List<Page> pages = in.readCollectionAsList(Page::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            profile = in.readOptionalWriteable(Profile::new);
        }
        boolean columnar = in.readBoolean();
        return new EsqlQueryResponse(columns, pages, profile, columnar, asyncExecutionId, isRunning, isAsync);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_ASYNC_QUERY)) {
            out.writeOptionalString(asyncExecutionId);
            out.writeBoolean(isRunning);
            out.writeBoolean(isAsync);
        }
        out.writeCollection(columns);
        out.writeCollection(pages);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalWriteable(profile);
        }
        out.writeBoolean(columnar);
    }

    public List<ColumnInfo> columns() {
        return columns;
    }

    List<Page> pages() {
        return pages;
    }

    public Iterator<Iterator<Object>> values() {
        List<String> dataTypes = columns.stream().map(ColumnInfo::type).toList();
        return ResponseValueUtils.pagesToValues(dataTypes, pages);
    }

    public Iterable<Iterable<Object>> rows() {
        List<String> dataTypes = columns.stream().map(ColumnInfo::type).toList();
        return ResponseValueUtils.valuesForRowsInPages(dataTypes, pages);
    }

    public Iterator<Object> column(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columns.size()) throw new IllegalArgumentException();
        return ResponseValueUtils.valuesForColumn(columnIndex, columns.get(columnIndex).type(), pages);
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

    private Iterator<? extends ToXContent> asyncPropertiesOrEmpty() {
        if (isAsync) {
            return ChunkedToXContentHelper.singleChunk((builder, params) -> {
                if (asyncExecutionId != null) {
                    builder.field("id", asyncExecutionId);
                }
                builder.field("is_running", isRunning);
                return builder;
            });
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        boolean dropNullColumns = params.paramAsBoolean(DROP_NULL_COLUMNS_OPTION, false);
        boolean[] nullColumns = dropNullColumns ? nullColumns() : null;
        Iterator<? extends ToXContent> columnHeadings = dropNullColumns
            ? Iterators.concat(
                ResponseXContentUtils.allColumns(columns, "all_columns"),
                ResponseXContentUtils.nonNullColumns(columns, nullColumns, "columns")
            )
            : ResponseXContentUtils.allColumns(columns, "columns");
        Iterator<? extends ToXContent> valuesIt = ResponseXContentUtils.columnValues(this.columns, this.pages, columnar, nullColumns);
        Iterator<ToXContent> profileRender = profile == null
            ? List.<ToXContent>of().iterator()
            : ChunkedToXContentHelper.field("profile", profile, params);
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            asyncPropertiesOrEmpty(),
            columnHeadings,
            ChunkedToXContentHelper.array("values", valuesIt),
            profileRender,
            ChunkedToXContentHelper.endObject()
        );
    }

    private boolean[] nullColumns() {
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
    public boolean isFragment() {
        return false;
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
            && Objects.equals(profile, that.profile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            asyncExecutionId,
            isRunning,
            columns,
            Iterators.hashCode(values(), row -> Iterators.hashCode(row, Objects::hashCode)),
            columnar
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

    public static class Profile implements Writeable, ChunkedToXContentObject {
        private final List<DriverProfile> drivers;

        public Profile(List<DriverProfile> drivers) {
            this.drivers = drivers;
        }

        public Profile(StreamInput in) throws IOException {
            this.drivers = in.readCollectionAsImmutableList(DriverProfile::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(drivers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Profile profile = (Profile) o;
            return Objects.equals(drivers, profile.drivers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(drivers);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ChunkedToXContentHelper.array("drivers", drivers.iterator(), params),
                ChunkedToXContentHelper.endObject()
            );
        }

        List<DriverProfile> drivers() {
            return drivers;
        }
    }
}
