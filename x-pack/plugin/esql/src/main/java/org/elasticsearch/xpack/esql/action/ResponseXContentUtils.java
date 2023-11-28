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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Collection of static utility methods for helping transform response data to XContent.
 */
final class ResponseXContentUtils {

    /** Returns the column headings for the given columns. */
    static Iterator<? extends ToXContent> columnHeadings(List<ColumnInfo> columns) {
        return ChunkedToXContentHelper.singleChunk((builder, params) -> {
            builder.startArray("columns");
            for (ColumnInfo col : columns) {
                col.toXContent(builder, params);
            }
            return builder.endArray();
        });
    }

    /** Returns the column values for the given pages (described by the column infos). */
    static Iterator<? extends ToXContent> columnValues(List<ColumnInfo> columns, List<Page> pages, boolean columnar) {
        if (pages.isEmpty()) {
            return Collections.emptyIterator();
        } else if (columnar) {
            return columnarValues(columns, pages);
        } else {
            return rowValues(columns, pages);
        }
    }

    /** Returns a columnar based representation of the values in the given pages (described by the column infos). */
    static Iterator<? extends ToXContent> columnarValues(List<ColumnInfo> columns, List<Page> pages) {
        final BytesRef scratch = new BytesRef();
        return Iterators.flatMap(
            Iterators.forRange(
                0,
                columns.size(),
                column -> Iterators.concat(
                    Iterators.single(((builder, params) -> builder.startArray())),
                    Iterators.flatMap(pages.iterator(), page -> {
                        ColumnInfo.PositionToXContent toXContent = columns.get(column).positionToXContent(page.getBlock(column), scratch);
                        return Iterators.forRange(
                            0,
                            page.getPositionCount(),
                            position -> (builder, params) -> toXContent.positionToXContent(builder, params, position)
                        );
                    }),
                    ChunkedToXContentHelper.endArray()
                )
            ),
            Function.identity()
        );
    }

    /** Returns a row based representation of the values in the given pages (described by the column infos). */
    static Iterator<? extends ToXContent> rowValues(List<ColumnInfo> columns, List<Page> pages) {
        final BytesRef scratch = new BytesRef();
        return Iterators.flatMap(pages.iterator(), page -> {
            final int columnCount = columns.size();
            assert page.getBlockCount() == columnCount : page.getBlockCount() + " != " + columnCount;
            final ColumnInfo.PositionToXContent[] toXContents = new ColumnInfo.PositionToXContent[columnCount];
            for (int column = 0; column < columnCount; column++) {
                toXContents[column] = columns.get(column).positionToXContent(page.getBlock(column), scratch);
            }
            return Iterators.forRange(0, page.getPositionCount(), position -> (builder, params) -> {
                builder.startArray();
                for (int c = 0; c < columnCount; c++) {
                    toXContents[c].positionToXContent(builder, params, position);
                }
                return builder.endArray();
            });
        });
    }
}
