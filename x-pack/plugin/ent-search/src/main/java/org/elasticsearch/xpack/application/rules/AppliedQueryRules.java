/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.xpack.searchbusinessrules.SpecifiedDocument;

import java.util.ArrayList;
import java.util.List;

public class AppliedQueryRules {

    private final List<SpecifiedDocument> pinnedDocs;
    private final List<SpecifiedDocument> excludedDocs;

    public AppliedQueryRules() {
        this(new ArrayList<>(0), new ArrayList<>(0));
    }

    public AppliedQueryRules(List<SpecifiedDocument> pinnedDocs, List<SpecifiedDocument> excludedDocs) {
        this.pinnedDocs = pinnedDocs;
        this.excludedDocs = excludedDocs;
    }

    public List<SpecifiedDocument> pinnedDocs() {
        return pinnedDocs;
    }

    public List<SpecifiedDocument> excludedDocs() {
        return excludedDocs;
    }

}
