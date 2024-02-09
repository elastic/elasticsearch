/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.PinnedDocument;

public class AppliedQueryRules {
    private final List<PinnedDocument> pinnedDocs;

    public AppliedQueryRules() {
        this(new ArrayList<>(0));
    }

    public AppliedQueryRules(List<PinnedDocument> pinnedDocs) {
        this.pinnedDocs = pinnedDocs;
    }

    public List<PinnedDocument> pinnedDocs() {
        return pinnedDocs;
    }

}
