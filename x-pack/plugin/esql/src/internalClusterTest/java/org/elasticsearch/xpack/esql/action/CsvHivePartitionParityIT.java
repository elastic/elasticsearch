/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

/** CSV leg of the hive-partition parity sweep (immune to both fixed bugs; guards parity as a regression net). */
public class CsvHivePartitionParityIT extends AbstractDelimitedHivePartitionParityIT {

    @Override
    protected String extension() {
        return "csv";
    }

    @Override
    protected char delimiter() {
        return ',';
    }
}
