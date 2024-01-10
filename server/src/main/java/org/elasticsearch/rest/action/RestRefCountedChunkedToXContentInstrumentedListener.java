/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.action.search.ReportsTookTime;
import org.elasticsearch.rest.action.search.SearchRestMetrics;

/**
 * Same as {@link RestRefCountedChunkedToXContentListener} but recordings metrics of timing data of classes that implement ReportsTookTime.
 */
public class RestRefCountedChunkedToXContentInstrumentedListener<Response extends ChunkedToXContent & RefCounted & ReportsTookTime> extends
    RestRefCountedChunkedToXContentListener<Response> {
    SearchRestMetrics searchRestMetrics;

    public RestRefCountedChunkedToXContentInstrumentedListener(RestChannel channel, SearchRestMetrics searchRestMetrics) {
        super(channel);
        this.searchRestMetrics = searchRestMetrics;
    }

    @Override
    public void onResponse(Response response) {
        searchRestMetrics.getSearchRequestCount().increment();
        searchRestMetrics.getSearchDurationTotalMillisCount().incrementBy(response.getTookMillis());
        searchRestMetrics.getSearchDurationsMillisHistogram().record(response.getTookMillis());
        super.onResponse(response);
    }
}
