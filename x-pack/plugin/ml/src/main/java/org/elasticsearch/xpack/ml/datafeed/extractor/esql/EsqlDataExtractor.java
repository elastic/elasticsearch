/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;

import java.io.IOException;

public class EsqlDataExtractor implements DataExtractor {

    @Override
    public DataSummary getSummary() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Result next() throws IOException {
        return null;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public void cancel() {

    }

    @Override
    public void destroy() {

    }

    @Override
    public long getEndTime() {
        return 0;
    }
}
