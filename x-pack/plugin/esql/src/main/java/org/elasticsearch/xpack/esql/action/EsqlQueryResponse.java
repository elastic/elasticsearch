/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
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
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class EsqlQueryResponse extends ActionResponse implements ChunkedToXContentObject, Releasable {

    private final AbstractRefCounted counted = AbstractRefCounted.of(this::closeInternal);

    private static final ParseField ID = new ParseField("id");
    private static final ParseField IS_RUNNING = new ParseField("is_running");
    private static final InstantiatingObjectParser<EsqlQueryResponse, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<EsqlQueryResponse, Void> parser = InstantiatingObjectParser.builder(
            "esql/query_response",
            true,
            EsqlQueryResponse.class
        );
        parser.declareObjectArray(constructorArg(), (p, c) -> ColumnInfo.fromXContent(p), new ParseField("columns"));
        parser.declareField(constructorArg(), (p, c) -> p.list(), new ParseField("values"), ObjectParser.ValueType.OBJECT_ARRAY);
        parser.declareString(optionalConstructorArg(), ID);
        parser.declareBoolean(constructorArg(), IS_RUNNING);
        PARSER = parser.build();
    }

    private final List<ColumnInfo> columns;
    private final List<Page> pages;
    private final Profile profile;
    private final boolean columnar;
    private final String asyncExecutionId;
    private final boolean isRunning;

    public EsqlQueryResponse(
        List<ColumnInfo> columns,
        List<Page> pages,
        @Nullable Profile profile,
        boolean columnar,
        @Nullable String asyncExecutionId,
        boolean isRunning
    ) {
        this.columns = columns;
        this.pages = pages;
        this.profile = profile;
        this.columnar = columnar;
        this.asyncExecutionId = asyncExecutionId;
        this.isRunning = isRunning;
    }

    public EsqlQueryResponse(List<ColumnInfo> columns, List<Page> pages, @Nullable Profile profile, boolean columnar) {
        this(columns, pages, profile, columnar, null, false);
    }

    /**
     * Build a reader for the response.
     */
    public static Writeable.Reader<EsqlQueryResponse> reader(BlockFactory blockFactory) {
        return in -> deserialize(new BlockStreamInput(in, blockFactory));
    }

    static EsqlQueryResponse deserialize(BlockStreamInput in) throws IOException {
        String asyncExecutionId = null;
        boolean isRunning = false;
        Profile profile = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_ASYNC_QUERY)) {
            asyncExecutionId = in.readOptionalString();
            isRunning = in.readBoolean();
        }
        List<ColumnInfo> columns = in.readCollectionAsList(ColumnInfo::new);
        List<Page> pages = in.readCollectionAsList(Page::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE)) {
            profile = in.readOptionalWriteable(Profile::new);
        }
        boolean columnar = in.readBoolean();
        return new EsqlQueryResponse(columns, pages, profile, columnar, asyncExecutionId, isRunning);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_ASYNC_QUERY)) {
            out.writeOptionalString(asyncExecutionId);
            out.writeBoolean(isRunning);
        }
        out.writeCollection(columns);
        out.writeCollection(pages);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE)) {
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

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        final Iterator<? extends ToXContent> valuesIt = ResponseXContentUtils.columnValues(this.columns, this.pages, columnar);
        Iterator<? extends ToXContent> idIt = Collections.emptyIterator();
        if (asyncExecutionId != null) {
            idIt = ChunkedToXContentHelper.field("id", asyncExecutionId);
        }
        Iterator<ToXContent> profileRender = profile == null
            ? List.<ToXContent>of().iterator()
            : ChunkedToXContentHelper.field("profile", profile, params);
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            idIt,
            ChunkedToXContentHelper.field("is_running", isRunning),
            ResponseXContentUtils.columnHeadings(columns),
            ChunkedToXContentHelper.array("values", valuesIt),
            profileRender,
            ChunkedToXContentHelper.endObject()
        );
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    public static EsqlQueryResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
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
        decRef();
    }

    void closeInternal() {
        Releasables.close(() -> Iterators.map(pages.iterator(), p -> p::releaseBlocks));
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
