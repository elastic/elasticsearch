/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.index.IndexSettings;

/**
 * An analysis component whose validation depends on where it was defined. {@link AnalysisRegistry} calls exactly one
 * of these methods after building the component.
 */
public interface DefinitionValidatingFactory {

    /** Validates a component defined in index settings. */
    void validateIndexDefinition(IndexSettings indexSettings);

    /** Validates a component defined inline in a request, e.g. {@code _analyze} or an ML categorization analyzer. */
    void validateInlineDefinition(IndexSettings indexSettings);
}
