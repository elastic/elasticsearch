/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;

/**
 * A bucket in the histogram where documents fall in
 */
public abstract class AbstractHistogramBucket extends InternalMultiBucketAggregation.InternalBucketWritable {

    protected final long docCount;
    protected final InternalAggregations aggregations;
    protected final transient DocValueFormat format;

    protected AbstractHistogramBucket(long docCount, InternalAggregations aggregations, DocValueFormat format) {
        this.docCount = docCount;
        this.aggregations = aggregations;
        this.format = format;
    }

    @Override
    public final long getDocCount() {
        return docCount;
    }

    @Override
    public final InternalAggregations getAggregations() {
        return aggregations;
    }

    public final DocValueFormat getFormatter() {
        return format;
    }
}
