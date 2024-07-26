/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.Item;

public class AppliedQueryRules {

    private final List<Item> pinnedDocs;
    private final List<Item> excludedDocs;

    public AppliedQueryRules() {
        this(new ArrayList<>(0), new ArrayList<>(0));
    }

    public AppliedQueryRules(List<Item> pinnedDocs, List<Item> excludedDocs) {
        this.pinnedDocs = pinnedDocs;
        this.excludedDocs = excludedDocs;
    }

    public List<Item> pinnedDocs() {
        return pinnedDocs;
    }

    public List<Item> excludedDocs() {
        return excludedDocs;
    }

}
