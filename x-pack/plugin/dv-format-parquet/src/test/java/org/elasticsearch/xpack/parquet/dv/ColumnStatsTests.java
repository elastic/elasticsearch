/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

/** The columns that get a skipper, and the factor between the value that the file stores and the value that the reader returns. */
public class ColumnStatsTests extends ESTestCase {

    public void testScaleOfTheColumnsThatTheReaderConverts() {
        assertThat(
            ExternalFilePiece.ColumnStats.readerScale(PrimitiveTypeName.INT32, ParquetFileInfo.LogicalKind.DATE),
            equalTo(86_400_000L)
        );
        assertThat(
            ExternalFilePiece.ColumnStats.readerScale(PrimitiveTypeName.INT64, ParquetFileInfo.LogicalKind.TIMESTAMP_MICROS),
            equalTo(1_000L)
        );
        assertThat(
            ExternalFilePiece.ColumnStats.readerScale(PrimitiveTypeName.INT64, ParquetFileInfo.LogicalKind.TIMESTAMP_MILLIS),
            equalTo(1L)
        );
        assertThat(
            ExternalFilePiece.ColumnStats.readerScale(PrimitiveTypeName.INT64, ParquetFileInfo.LogicalKind.TIMESTAMP_NANOS),
            equalTo(1L)
        );
        for (ParquetFileInfo.LogicalKind plain : new ParquetFileInfo.LogicalKind[] {
            ParquetFileInfo.LogicalKind.NONE,
            ParquetFileInfo.LogicalKind.INT_8,
            ParquetFileInfo.LogicalKind.INT_16,
            ParquetFileInfo.LogicalKind.UINT_16 }) {
            assertThat(ExternalFilePiece.ColumnStats.readerScale(PrimitiveTypeName.INT32, plain), equalTo(1L));
        }
    }

    public void testColumnsWhoseBoundsDoNotFollowTheReaderHaveNoSkipper() {
        // unsigned 32 and 64 bit columns are ordered as unsigned in the file and widened or encoded by the reader
        assertThat(ExternalFilePiece.ColumnStats.readerScale(PrimitiveTypeName.INT32, ParquetFileInfo.LogicalKind.UINT_32), equalTo(0L));
        assertThat(ExternalFilePiece.ColumnStats.readerScale(PrimitiveTypeName.INT64, ParquetFileInfo.LogicalKind.UINT_64), equalTo(0L));
        assertThat(ExternalFilePiece.ColumnStats.readerScale(PrimitiveTypeName.INT32, ParquetFileInfo.LogicalKind.DECIMAL), equalTo(0L));
        assertThat(ExternalFilePiece.ColumnStats.readerScale(PrimitiveTypeName.INT32, ParquetFileInfo.LogicalKind.TIME), equalTo(0L));
        // and the physical types that no numeric iterator serves
        for (PrimitiveTypeName type : new PrimitiveTypeName[] {
            PrimitiveTypeName.BINARY,
            PrimitiveTypeName.DOUBLE,
            PrimitiveTypeName.FLOAT,
            PrimitiveTypeName.BOOLEAN }) {
            assertThat(ExternalFilePiece.ColumnStats.readerScale(type, ParquetFileInfo.LogicalKind.NONE), equalTo(0L));
        }
    }
}
