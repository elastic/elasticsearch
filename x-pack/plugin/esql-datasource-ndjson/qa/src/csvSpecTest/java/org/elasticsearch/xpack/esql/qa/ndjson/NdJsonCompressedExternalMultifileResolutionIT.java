/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.ndjson;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

import java.util.List;

/**
 * Compressed-NDJSON csv-spec tests for {@code external-multifile-resolution.csv-spec}, run against every
 * configured compression format and storage backend. Split out of the former
 * {@code NdJsonCompressedFormatSpecIT} so each csv-spec file is its own junit suite (see
 * {@link AbstractNdJsonCompressedFormatSpecTestCase}).
 */
public class NdJsonCompressedExternalMultifileResolutionIT extends AbstractNdJsonCompressedFormatSpecTestCase {

    public NdJsonCompressedExternalMultifileResolutionIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        String format,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, format, storageBackend);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTestsWithFormats(COMPRESSED_FORMATS, "/external-multifile-resolution.csv-spec");
    }
}
