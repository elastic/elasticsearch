/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

/** TSV leg of the hive-partition parity sweep (served by the CSV reader with a tab delimiter). */
public class TsvHivePartitionParityIT extends AbstractDelimitedHivePartitionParityIT {

    @Override
    protected String extension() {
        return "tsv";
    }

    @Override
    protected char delimiter() {
        return '\t';
    }
}
