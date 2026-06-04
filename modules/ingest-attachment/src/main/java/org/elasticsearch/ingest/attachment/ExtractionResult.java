/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import java.util.Map;

/**
 * The result of extracting content and metadata from a document via an {@link ExtractionBackend}.
 *
 * @param content  the extracted plain text (may be empty but never {@code null}); raw output from
 *                 the parser, possibly including trailing whitespace
 * @param metadata a flat map of metadata key-to-value pairs; keys match the string representations
 *                 of Apache Tika property names (e.g. {@code "dcterms:created"}, {@code "Content-Type"})
 */
record ExtractionResult(String content, Map<String, String> metadata) {}
