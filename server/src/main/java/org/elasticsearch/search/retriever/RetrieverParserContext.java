/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import java.util.function.Consumer;

public class RetrieverParserContext {

    protected Consumer<String> trackSectionUsage;
    protected Consumer<String> trackQueryUsage;
    protected Consumer<String> trackRescorerUsage;

    public RetrieverParserContext() {

    }

    public RetrieverParserContext(
        Consumer<String> trackSectionUsage,
        Consumer<String> trackQueryUsage,
        Consumer<String> trackRescorerUsage
    ) {
        this.trackSectionUsage = trackSectionUsage;
        this.trackQueryUsage = trackQueryUsage;
    }

    public void trackSectionUsage(String section) {
        if (trackSectionUsage != null) {
            trackSectionUsage.accept(section);
        }
    }

    public void trackQueryUsage(String query) {
        if (trackQueryUsage != null) {
            trackQueryUsage.accept(query);
        }
    }

    public void trackRescorerUsage(String name) {
        if (trackRescorerUsage != null) {
            trackRescorerUsage.accept(name);
        }
    }
}
