/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort.plugin;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.Collections;
import java.util.List;

public class CustomSortPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<SortSpec<?>> getSorts() {
        return Collections.singletonList(new SortSpec<>(CustomSortBuilder.NAME, CustomSortBuilder::new, CustomSortBuilder::fromXContent));
    }
}
