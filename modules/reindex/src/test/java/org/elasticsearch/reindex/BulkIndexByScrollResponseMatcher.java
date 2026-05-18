/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Collection;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@SuppressWarnings("HiddenField")
public class BulkIndexByScrollResponseMatcher extends TypeSafeMatcher<BulkByScrollResponse> {

    private Matcher<Long> createdMatcher = equalTo(0L);
    private Matcher<Long> updatedMatcher = equalTo(0L);
    private Matcher<Long> deletedMatcher = equalTo(0L);

    /// Matches for the `total` field on the response (number of matching source documents the reindex/UBQ/DBQ task
    /// planned to process). Optional, because most existing tests only care about the running counters.
    private Matcher<Long> totalMatcher;

    /**
     * Matches for number of batches. Optional.
     */
    private Matcher<Integer> batchesMatcher;
    private Matcher<Long> versionConflictsMatcher = equalTo(0L);
    private Matcher<Integer> failuresMatcher = equalTo(0);
    private Matcher<String> reasonCancelledMatcher = nullValue(String.class);
    private Matcher<Collection<? extends BulkIndexByScrollResponseMatcher>> slicesMatcher = empty();

    public BulkIndexByScrollResponseMatcher created(Matcher<Long> createdMatcher) {
        this.createdMatcher = createdMatcher;
        return this;
    }

    public BulkIndexByScrollResponseMatcher created(long created) {
        return created(equalTo(created));
    }

    public BulkIndexByScrollResponseMatcher updated(Matcher<Long> updatedMatcher) {
        this.updatedMatcher = updatedMatcher;
        return this;
    }

    public BulkIndexByScrollResponseMatcher updated(long updated) {
        return updated(equalTo(updated));
    }

    public BulkIndexByScrollResponseMatcher deleted(Matcher<Long> deletedMatcher) {
        this.deletedMatcher = deletedMatcher;
        return this;
    }

    public BulkIndexByScrollResponseMatcher deleted(long deleted) {
        return deleted(equalTo(deleted));
    }

    /// Set the matcher for the `total` field on the response (the planned number of source documents to process).
    public BulkIndexByScrollResponseMatcher total(Matcher<Long> totalMatcher) {
        this.totalMatcher = totalMatcher;
        return this;
    }

    public BulkIndexByScrollResponseMatcher total(long total) {
        return total(equalTo(total));
    }

    /**
     * Set the matches for the number of batches. Defaults to matching any
     * integer because we usually don't care about how many batches the job
     * takes.
     */
    public BulkIndexByScrollResponseMatcher batches(Matcher<Integer> batchesMatcher) {
        this.batchesMatcher = batchesMatcher;
        return this;
    }

    public BulkIndexByScrollResponseMatcher batches(int batches) {
        return batches(equalTo(batches));
    }

    public BulkIndexByScrollResponseMatcher batches(int total, int batchSize) {
        // Round up
        return batches((total + batchSize - 1) / batchSize);
    }

    public BulkIndexByScrollResponseMatcher versionConflicts(Matcher<Long> versionConflictsMatcher) {
        this.versionConflictsMatcher = versionConflictsMatcher;
        return this;
    }

    public BulkIndexByScrollResponseMatcher versionConflicts(long versionConflicts) {
        return versionConflicts(equalTo(versionConflicts));
    }

    /**
     * Set the matcher for the size of the failures list. For more in depth
     * matching do it by hand. The type signatures required to match the
     * actual failures list here just don't work.
     */
    public BulkIndexByScrollResponseMatcher failures(Matcher<Integer> failuresMatcher) {
        this.failuresMatcher = failuresMatcher;
        return this;
    }

    /**
     * Set the expected size of the failures list.
     */
    public BulkIndexByScrollResponseMatcher failures(int failures) {
        return failures(equalTo(failures));
    }

    public BulkIndexByScrollResponseMatcher reasonCancelled(Matcher<String> reasonCancelledMatcher) {
        this.reasonCancelledMatcher = reasonCancelledMatcher;
        return this;
    }

    /**
     * Set the matcher for the workers portion of the response.
     */
    public BulkIndexByScrollResponseMatcher slices(Matcher<Collection<? extends BulkIndexByScrollResponseMatcher>> slicesMatcher) {
        this.slicesMatcher = slicesMatcher;
        return this;
    }

    @Override
    protected boolean matchesSafely(BulkByScrollResponse item) {
        return updatedMatcher.matches(item.getUpdated())
            && createdMatcher.matches(item.getCreated())
            && deletedMatcher.matches(item.getDeleted())
            && (totalMatcher == null || totalMatcher.matches(item.getStatus().getTotal()))
            && (batchesMatcher == null || batchesMatcher.matches(item.getBatches()))
            && versionConflictsMatcher.matches(item.getVersionConflicts())
            && failuresMatcher.matches(item.getBulkFailures().size())
            && reasonCancelledMatcher.matches(item.getReasonCancelled())
            && slicesMatcher.matches(item.getStatus().getSliceStatuses());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("updated matches ").appendDescriptionOf(updatedMatcher);
        description.appendText(" and created matches ").appendDescriptionOf(createdMatcher);
        description.appendText(" and deleted matches ").appendDescriptionOf(deletedMatcher);
        if (totalMatcher != null) {
            description.appendText(" and total matches ").appendDescriptionOf(totalMatcher);
        }
        if (batchesMatcher != null) {
            description.appendText(" and batches matches ").appendDescriptionOf(batchesMatcher);
        }
        description.appendText(" and versionConflicts matches ").appendDescriptionOf(versionConflictsMatcher);
        description.appendText(" and failures size matches ").appendDescriptionOf(failuresMatcher);
        description.appendText(" and reason cancelled matches ").appendDescriptionOf(reasonCancelledMatcher);
        description.appendText(" and slices matches ").appendDescriptionOf(slicesMatcher);
    }
}
