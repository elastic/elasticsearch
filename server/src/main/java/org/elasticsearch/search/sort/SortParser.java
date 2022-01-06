/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Defines a sort parser that is able to parse {@link SortBuilder}s from {@link org.elasticsearch.xcontent.XContent}.
 */
@FunctionalInterface
public interface SortParser<SB extends SortBuilder<SB>> {
    /**
     * Creates a new {@link SortBuilder} from the sort held by the
     * {@link XContentParser}. The state on the parser contained in this context
     * will be changed as a side effect of this method call
     */
    SB fromXContent(XContentParser parser, String elementName) throws IOException;
}
