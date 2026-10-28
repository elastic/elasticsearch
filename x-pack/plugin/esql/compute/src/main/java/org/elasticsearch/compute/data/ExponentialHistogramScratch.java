/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;

/**
 * Reusable storage to be passed to {@link ExponentialHistogramBlock#getExponentialHistogram(int, ExponentialHistogramScratch)}.
 */
public class ExponentialHistogramScratch {

    final BytesRef bytesRefScratch = new BytesRef();
    final CompressedExponentialHistogram reusedHistogram = new CompressedExponentialHistogram();

}
