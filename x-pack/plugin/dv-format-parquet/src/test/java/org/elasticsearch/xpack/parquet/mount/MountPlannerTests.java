/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.parquet.docvalues.BytesParquetObject;
import org.elasticsearch.xpack.parquet.docvalues.ParquetFileInfo;
import org.elasticsearch.xpack.parquet.docvalues.ParquetFooterReader;
import org.elasticsearch.xpack.parquet.docvalues.ParquetTestFiles;
import org.elasticsearch.xpack.parquet.mount.MountPlanner.IndexPlan;
import org.elasticsearch.xpack.parquet.mount.MountPlanner.Piece;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class MountPlannerTests extends ESTestCase {

    private static ParquetFileInfo file(String name, int rows, int rowGroupRows) throws IOException {
        return ParquetFooterReader.read(
            new BytesParquetObject("repository://lake/" + name, ParquetTestFiles.writeFlat(rows, rowGroupRows))
        );
    }

    public void testOneFileInOneIndexWhenItFits() throws IOException {
        List<IndexPlan> plan = MountPlanner.plan(List.of(file("a.parquet", 1_000, 300)), 10_000);
        assertEquals(List.of(new IndexPlan(List.of(new Piece(0, 0, 4, 1_000)), 1_000)), plan);
    }

    public void testAFileSpansIndicesAtRowGroupBorders() throws IOException {
        // row groups of 300, 300, 300 and 100 rows with a limit of 700: the third row group does not fit in the first index
        List<IndexPlan> plan = MountPlanner.plan(List.of(file("a.parquet", 1_000, 300)), 700);
        assertEquals(
            List.of(new IndexPlan(List.of(new Piece(0, 0, 2, 600)), 600), new IndexPlan(List.of(new Piece(0, 2, 2, 400)), 400)),
            plan
        );
    }

    public void testSeveralFilesFillIndicesInRequestOrder() throws IOException {
        // a: 300 + 300 + 300 + 100, b: 250 + 250 + 100, limit 800
        List<IndexPlan> plan = MountPlanner.plan(List.of(file("a.parquet", 1_000, 300), file("b.parquet", 600, 250)), 800);
        assertEquals(
            List.of(
                new IndexPlan(List.of(new Piece(0, 0, 2, 600)), 600),
                // the third row group of a starts the second index, and the fourth (100) and b (250) fit next to it: 300 + 100 + 250 = 650
                new IndexPlan(List.of(new Piece(0, 2, 2, 400), new Piece(1, 0, 1, 250)), 650),
                new IndexPlan(List.of(new Piece(1, 1, 2, 350)), 350)
            ),
            plan
        );
        assertEquals(1_600, plan.stream().mapToLong(IndexPlan::rows).sum());
    }

    public void testARowGroupLargerThanTheLimitFailsTheMount() throws IOException {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MountPlanner.plan(List.of(file("a.parquet", 1_000, 300)), 299)
        );
        assertThat(e.getMessage(), containsString("has [300] rows, more than max_rows_per_index [299]"));
        expectThrows(IllegalArgumentException.class, () -> MountPlanner.plan(List.of(), 0));
        assertTrue(MountPlanner.plan(List.of(), 10).isEmpty());
    }

    public void testRandomPlansKeepEveryRowOnceInOrder() throws IOException {
        List<ParquetFileInfo> files = new java.util.ArrayList<>();
        long total = 0;
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            int rows = randomIntBetween(1, 2_000);
            files.add(file("f" + i + ".parquet", rows, randomIntBetween(1, 400)));
            total += rows;
        }
        long limit = randomLongBetween(400, 3_000);
        List<IndexPlan> plan = MountPlanner.plan(files, limit);
        assertEquals(total, plan.stream().mapToLong(IndexPlan::rows).sum());
        int nextFile = 0;
        int nextRowGroup = 0;
        for (IndexPlan index : plan) {
            assertTrue(index.rows() <= limit);
            for (Piece piece : index.pieces()) {
                // pieces follow each other through the row groups of the files
                if (piece.file() != nextFile) {
                    assertEquals(nextFile + 1, piece.file());
                    assertEquals(files.get(nextFile).rowGroups().size(), nextRowGroup);
                    nextFile = piece.file();
                    nextRowGroup = 0;
                }
                assertEquals(nextRowGroup, piece.firstRowGroup());
                nextRowGroup += piece.rowGroupCount();
            }
        }
        assertEquals(files.size() - 1, nextFile);
        assertEquals(files.get(nextFile).rowGroups().size(), nextRowGroup);
    }
}
