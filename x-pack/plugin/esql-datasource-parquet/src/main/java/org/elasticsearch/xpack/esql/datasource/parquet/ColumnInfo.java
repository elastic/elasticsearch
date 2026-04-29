/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Per-column metadata used by the Parquet column iterators. Holds the Parquet column descriptor
 * alongside the ESQL type mapping and definition/repetition levels.
 */
record ColumnInfo(
    ColumnDescriptor descriptor,
    PrimitiveType.PrimitiveTypeName parquetType,
    DataType esqlType,
    int maxDefLevel,
    int maxRepLevel,
    LogicalTypeAnnotation logicalType
) {}
