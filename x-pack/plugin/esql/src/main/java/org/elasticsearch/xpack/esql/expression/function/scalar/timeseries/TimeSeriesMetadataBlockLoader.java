/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Loads {@code _timeseries} as a typed label container straight from the doc values of the dimensions: each dimension a label,
 * written as {@link TimeSeriesMetadataEncoding} holds it. Nothing is parsed or rendered on the way: a keyword value is its bytes,
 * an ip, a number or a boolean its text. A time-series aggregate reads it for the first document of each series of a page
 * ({@code ReadDimsExec}), which keeps the page dictionary encoded by series, so a series' labels load once per page.
 * <p>
 * TODO: an empty value is left out, so a series whose label is empty and one that lacks the label have the same label
 * container, and a time-series aggregate keyed by it merges their samples. Prometheus has no empty label, so data written by
 * it never holds such a pair; data written otherwise may, and answers then differ from what each series alone gives. Not
 * supported for now.
 */
public final class TimeSeriesMetadataBlockLoader implements BlockLoader {
    /** How the values of a dimension are written as label values. */
    public enum Values {
        /** The bytes of a keyword, the text of a number or a boolean, as the block holds it. */
        AS_LOADED,
        /** The text of an ip address. */
        IP,
        /** The text of an unsigned long. */
        UNSIGNED_LONG
    }

    /** A dimension: the label PromQL names it, how it loads, and how its values are written. */
    public record Dimension(String label, BlockLoader loader, Values values) {}

    /** The dimensions, in label name order. */
    private final List<Dimension> dimensions;
    private final List<BytesRef> labels;

    public TimeSeriesMetadataBlockLoader(List<Dimension> dimensions) {
        this.dimensions = dimensions.stream().sorted(Comparator.comparing(d -> new BytesRef(d.label()))).toList();
        this.labels = this.dimensions.stream().map(d -> new BytesRef(d.label())).toList();
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) throws IOException {
        var readers = new ArrayList<IOFunction<CircuitBreaker, ColumnAtATimeReader>>(dimensions.size());
        for (Dimension dimension : dimensions) {
            IOFunction<CircuitBreaker, ColumnAtATimeReader> reader = dimension.loader().columnAtATimeReader(context);
            if (reader == null) {
                // A dimension has doc values, which load column at a time.
                throw new IllegalStateException("dimension [" + dimension.label() + "] does not load column at a time");
            }
            readers.add(reader);
        }
        return breaker -> new TimeSeriesMetadataColumnReader(breaker, dimensions, labels, readers);
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) {
        // every dimension loads column at a time, see columnAtATimeReader
        return null;
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return StoredFieldsSpec.NO_REQUIREMENTS;
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) {
        throw new UnsupportedOperationException("_timeseries does not load as ordinals");
    }

    @Override
    public String toString() {
        return "TimeSeriesMetadata" + labels.stream().map(BytesRef::utf8ToString).toList();
    }
}
