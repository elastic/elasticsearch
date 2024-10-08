/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.coordinator;

import org.elasticsearch.search.profile.AbstractProfileBreakdown;
import org.elasticsearch.search.profile.SearchProfileCoordinatorResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SearchCoordinatorProfiler extends AbstractProfileBreakdown<SearchCoordinatorTimingType> {

    List<RetrieverProfileResult> retrieversProfile = new ArrayList<>();

    public SearchCoordinatorProfiler() {
        super(SearchCoordinatorTimingType.class);
    }

    public void captureRetrieverResult(RetrieverProfileResult profileResult) {
        retrieversProfile.add(profileResult);
    }

    public void captureRetrieverDetails(Map<String, SearchProfileCoordinatorResult> profileResults) {

    }
}
