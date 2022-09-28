/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.index.Index;

import java.util.Collections;
import java.util.List;

public class SearchEngine {

    public String getName() {
        // TODO: implement.
        return null;
    }

    public List<Index> getIndices() {
        // TODO: implement.
        return Collections.emptyList();
    }

    public boolean isHidden() {
        // TODO: implement.
        return false;
    }

    public boolean isSystem() {
        // TODO: implement.
        return false;
    }
}
