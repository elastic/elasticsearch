/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.compute.data.BlockFactory;

import java.util.List;

/**
 * TSV format reader — a {@link CsvFormatReader} pre-configured with tab delimiter and
 * {@link CsvFormatOptions#TSV} options. Recognised extensions: {@code .tsv}.
 */
public class TsvFormatReader extends CsvFormatReader {
    public TsvFormatReader(BlockFactory blockFactory) {
        super(blockFactory, CsvFormatOptions.TSV, "tsv", List.of(".tsv"));
    }
}
