/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Fetch sub phase that extracts significant portions of string fields, marking the matches. Pluggable by implementing
 * {@link org.elasticsearch.search.fetch.subphase.highlight.Highlighter} and
 * {@link org.elasticsearch.plugins.SearchPlugin#getHighlighters()}.
 */
package org.elasticsearch.search.fetch.subphase.highlight;
