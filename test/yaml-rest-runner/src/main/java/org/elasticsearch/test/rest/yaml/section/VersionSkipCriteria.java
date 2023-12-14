/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import java.util.List;

class VersionSkipCriteria implements SkipCriteria {
    private final List<VersionRange> versionRanges;

    VersionSkipCriteria(String versionRange) {
        this.versionRanges = VersionRange.parseVersionRanges(versionRange);
        assert versionRanges.isEmpty() == false;
    }

    @Override
    public boolean skip(SkipSectionContext context) {
        return versionRanges.stream().anyMatch(context::clusterVersionInRange);
    }
}
