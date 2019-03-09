/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.index.reindex.BulkByScrollTask.StatusBuilder;

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
            for (Object object: failures) {
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
