/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.search.internal.SearchContext;

public interface SlowLogMessageFactory {
    ESLogMessage indexSlowLogMessage(Index index, ParsedDocument doc, long tookInNanos, boolean reformat, int maxSourceCharsToLog);

    ESLogMessage searchSlowLogMessage(SearchContext context, long tookInNanos);
}
