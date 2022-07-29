/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollTask.StatusBuilder;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.xcontent.ObjectParser;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Helps build a {@link BulkByScrollResponse}. Used by an instance of {@link ObjectParser} when parsing from XContent.
 */
class BulkByScrollResponseBuilder extends StatusBuilder {
    private TimeValue took;
    private BulkByScrollTask.Status status;
    private List<Failure> bulkFailures = new ArrayList<>();
    private List<SearchFailure> searchFailures = new ArrayList<>();
    private boolean timedOut;

    BulkByScrollResponseBuilder() {}

    public void setTook(long took) {
        setTook(new TimeValue(took, TimeUnit.MILLISECONDS));
    }

    public void setTook(TimeValue took) {
        this.took = took;
    }

    public void setStatus(BulkByScrollTask.Status status) {
        this.status = status;
    }

    public void setFailures(List<Object> failures) {
        if (failures != null) {
            for (Object object : failures) {
                if (object instanceof Failure) {
                    bulkFailures.add((Failure) object);
                } else if (object instanceof SearchFailure) {
                    searchFailures.add((SearchFailure) object);
                }
            }
        }
    }

    public void setTimedOut(boolean timedOut) {
        this.timedOut = timedOut;
    }

    public BulkByScrollResponse buildResponse() {
        status = super.buildStatus();
        return new BulkByScrollResponse(took, status, bulkFailures, searchFailures, timedOut);
    }
}
