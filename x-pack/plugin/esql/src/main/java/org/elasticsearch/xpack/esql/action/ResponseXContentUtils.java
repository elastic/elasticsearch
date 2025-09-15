/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Collection of static utility methods for helping transform response data to XContent.
 */
final class ResponseXContentUtils {

    /**
     * Returns the column headings for the given columns.
     */
    static Iterator<? extends ToXContent> allColumns(List<ColumnInfoImpl> columns, String name) {
        return ChunkedToXContentHelper.singleChunk((builder, params) -> {
            builder.startArray(name);
            for (ColumnInfo col : columns) {
                col.toXContent(builder, params);
            }
            return builder.endArray();
        });
    }

    /**
     * Returns the column headings for the given columns, moving the heading
     * for always-null columns to a {@code null_columns} section.
     */
    static Iterator<? extends ToXContent> nonNullColumns(List<ColumnInfoImpl> columns, boolean[] nullColumns, String name) {
        return ChunkedToXContentHelper.singleChunk((builder, params) -> {
            builder.startArray(name);
            for (int c = 0; c < columns.size(); c++) {
                if (nullColumns[c] == false) {
                    columns.get(c).toXContent(builder, params);
                }
            }
            return builder.endArray();
        });
    }

    /** Returns the column values for the given pages (described by the column infos). */
    static Iterator<? extends ToXContent> columnValues(
        List<ColumnInfoImpl> columns,
        List<Page> pages,
        boolean columnar,
        boolean[] nullColumns
    ) {
        if (pages.isEmpty()) {
            return Collections.emptyIterator();
        } else if (columnar) {
            return columnarValues(columns, pages, nullColumns);
        } else {
            return rowValues(columns, pages, nullColumns);
        }
    }

    /** Returns a columnar based representation of the values in the given pages (described by the column infos). */
    static Iterator<? extends ToXContent> columnarValues(List<ColumnInfoImpl> columns, List<Page> pages, boolean[] nullColumns) {
        final BytesRef scratch = new BytesRef();
        return Iterators.flatMap(Iterators.forRange(0, columns.size(), column -> {
            if (nullColumns != null && nullColumns[column]) {
                return Collections.emptyIterator();
            }
            return Iterators.concat(
                Iterators.single(((builder, params) -> builder.startArray())),
                Iterators.flatMap(pages.iterator(), page -> {
                    PositionToXContent toXContent = PositionToXContent.positionToXContent(
                        columns.get(column),
                        page.getBlock(column),
                        scratch
                    );
                    return Iterators.forRange(
                        0,
                        page.getPositionCount(),
                        position -> (builder, params) -> toXContent.positionToXContent(builder, params, position)
                    );
                }),
                ChunkedToXContentHelper.endArray()
            );
        }), Function.identity());
    }

    /** Returns a row based representation of the values in the given pages (described by the column infos). */
    static Iterator<? extends ToXContent> rowValues(List<ColumnInfoImpl> columns, List<Page> pages, boolean[] nullColumns) {
        final BytesRef scratch = new BytesRef();
        return Iterators.flatMap(pages.iterator(), page -> {
            final int columnCount = columns.size();
            assert page.getBlockCount() == columnCount : page.getBlockCount() + " != " + columnCount;
            final PositionToXContent[] toXContents = new PositionToXContent[columnCount];
            for (int column = 0; column < columnCount; column++) {
                Block block = page.getBlock(column);
                toXContents[column] = PositionToXContent.positionToXContent(columns.get(column), block, scratch);
            }
            return Iterators.forRange(0, page.getPositionCount(), position -> (builder, params) -> {
                builder.startArray();
                for (int c = 0; c < columnCount; c++) {
                    if (nullColumns == null || nullColumns[c] == false) {
                        toXContents[c].positionToXContent(builder, params, position);
                    }
                }
                return builder.endArray();
            });
        });
    }
}
