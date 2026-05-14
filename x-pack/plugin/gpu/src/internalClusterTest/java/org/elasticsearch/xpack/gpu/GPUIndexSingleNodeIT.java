/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.test.ESIntegTestCase;

@LuceneTestCase.SuppressCodecs("*") // use our custom codec
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class GPUIndexSingleNodeIT extends BaseGPUIndexTestCase {}
