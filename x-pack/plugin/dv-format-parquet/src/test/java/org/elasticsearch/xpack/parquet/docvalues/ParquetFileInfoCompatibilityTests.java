/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/** What it takes for two Parquet files to be served together: the same mapped columns with the same types. */
public class ParquetFileInfoCompatibilityTests extends ESTestCase {

    private static ParquetFileInfo file(String name, int rows, int rowGroupRows) throws IOException {
        return ParquetFooterReader.read(
            new BytesParquetObject("repository://lake/" + name, ParquetTestFiles.writeFlat(rows, rowGroupRows))
        );
    }

    public void testFilesMustHaveTheMappedColumnsWithTheSameTypes() throws IOException {
        ParquetFileInfo flat = file("a.parquet", 10, 10);
        flat.requireCompatibleWith(file("b.parquet", 20, 5), List.of("id", "ts", "name"));
        ParquetFileInfo unsigned = ParquetFooterReader.read(
            new BytesParquetObject("repository://lake/u.parquet", ParquetTestFiles.writeUnsigned(5))
        );
        IllegalArgumentException missing = expectThrows(
            IllegalArgumentException.class,
            () -> flat.requireCompatibleWith(unsigned, List.of("id"))
        );
        assertThat(missing.getMessage(), containsString("has no column [id]"));
        ParquetFileInfo nested = ParquetFooterReader.read(
            new BytesParquetObject("repository://lake/n.parquet", ParquetTestFiles.writeNested(5))
        );
        // id is an int32 in both files: the nested file is compatible for it, but not for its struct column
        flat.requireCompatibleWith(nested, List.of("id"));
    }
}
