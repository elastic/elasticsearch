/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * ColumNAR is a Lucene {@link DocValuesFormat} for columnar, analytics-oriented workloads. Numeric
 * values are stored in a binary substrate and encoded per block by an adaptive pipeline (Native
 * Adaptive Representation). It is selected per field through {@link PerFieldDocValuesFormat}, so it
 * coexists with the default format field by field.
 */
package org.elasticsearch.columnar;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
