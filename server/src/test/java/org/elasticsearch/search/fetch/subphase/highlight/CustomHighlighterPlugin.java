/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;

import java.util.Map;

import static java.util.Collections.singletonMap;

public class CustomHighlighterPlugin extends Plugin implements SearchPlugin {
    @Override
    public Map<String, Highlighter> getHighlighters() {
        return singletonMap("test-custom", new CustomHighlighter());
    }
}
