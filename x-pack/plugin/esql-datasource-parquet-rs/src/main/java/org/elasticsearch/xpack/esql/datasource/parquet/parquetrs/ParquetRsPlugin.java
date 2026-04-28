/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.plugins.Plugin;

/**
 * Stub plugin for the parquet-rs native library integration. This plugin currently
 * serves as a placeholder so the module is recognized by the Elasticsearch distribution
 * build. The full DataSourcePlugin implementation will be added when the parquet-rs
 * reader is integrated into the ESQL data source framework.
 */
public class ParquetRsPlugin extends Plugin {}
