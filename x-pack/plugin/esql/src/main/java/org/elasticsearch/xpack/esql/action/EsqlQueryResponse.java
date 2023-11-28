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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
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

public class EsqlQueryResponse extends ActionResponse implements ChunkedToXContent, Releasable {

    private static final ParseField ID = new ParseField("id");
    private static final ParseField IS_RUNNING = new ParseField("is_running");

    private final String asyncExecutionId;
    private final boolean isRunning;
    private final List<ColumnInfo> columns;
    private final List<Page> pages;
    private final boolean columnar;

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

    public EsqlQueryResponse(
        List<ColumnInfo> columns,
        List<Page> pages,
        boolean columnar,
        @Nullable String asyncExecutionId,
        boolean isRunning
    ) {
        this.columns = columns;
        this.pages = pages;
        this.columnar = columnar;
        this.asyncExecutionId = asyncExecutionId;
        this.isRunning = isRunning;
    }

    public EsqlQueryResponse(List<ColumnInfo> columns, List<Page> pages, boolean columnar) {
        this(columns, pages, columnar, null, false);
    }

    public EsqlQueryResponse(
        List<ColumnInfo> columns,
        List<Page> pages,
        @Nullable String asyncExecutionId,
        boolean isRunning
    ) {
        this(columns, pages, false, asyncExecutionId, isRunning);
    }

    // TODO: can remove now that REST deser goes through the 4-arg variant
    public EsqlQueryResponse(List<ColumnInfo> columns, List<List<Object>> values) {
        this(columns, List.of(ResponseValueUtils.valuesToPage(columns, values)), false, null, false);
    }

    /**
     * Build a reader for the response.
     */
    public static Writeable.Reader<EsqlQueryResponse> reader(BlockFactory blockFactory) {
        return in -> new EsqlQueryResponse(new BlockStreamInput(in, blockFactory));
    }

    public EsqlQueryResponse(BlockStreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_ASYNC_QUERY)) {
            this.asyncExecutionId = in.readOptionalString();
            this.isRunning = in.readBoolean();
        } else {
            this.asyncExecutionId = null;
            this.isRunning = false;
        }
        this.columns = in.readCollectionAsList(ColumnInfo::new);
        this.pages = in.readCollectionAsList(Page::new);
        this.columnar = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_ASYNC_QUERY)) {
            out.writeOptionalString(asyncExecutionId);
            out.writeBoolean(isRunning);
        }
        out.writeCollection(columns);
        out.writeCollection(pages);
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
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params unused) {
        final Iterator<? extends ToXContent> valuesIt = ResponseXContentUtils.columnValues(this.columns, this.pages, columnar);
        Iterator<? extends ToXContent> idIt = Collections.emptyIterator();
        if (asyncExecutionId != null) {
            idIt = ChunkedToXContentHelper.field("id", asyncExecutionId);
        }
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            idIt,
            ChunkedToXContentHelper.field("is_running", isRunning),
            ResponseXContentUtils.columnHeadings(columns),
            ChunkedToXContentHelper.array("values", valuesIt),
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
        // TODO: add new properties
        return Objects.equals(columns, that.columns)
            && columnar == that.columnar
            && Iterators.equals(values(), that.values(), (row1, row2) -> Iterators.equals(row1, row2, Objects::equals));
    }

    @Override
    public int hashCode() {
        // TODO: add new properties
        return Objects.hash(columns, Iterators.hashCode(values(), row -> Iterators.hashCode(row, Objects::hashCode)), columnar);
    }

    @Override
    public String toString() {
        return Strings.toString(ChunkedToXContent.wrapAsToXContent(this));
    }

    @Override
    public void close() {
        Releasables.close(() -> Iterators.map(pages.iterator(), p -> p::releaseBlocks));
    }
}
