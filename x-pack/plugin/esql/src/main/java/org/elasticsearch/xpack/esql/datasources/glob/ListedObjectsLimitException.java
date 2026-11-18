/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.common.logging.LoggerMessageFormat;

/**
 * Thrown when a listing pulled more objects than {@code esql.external.max_listed_objects}.
 * Distinct from a bare {@link IllegalArgumentException} so the anchor probe keys on the type,
 * never on message text. Same REST status as {@code Check#clientError} (400).
 */
public final class ListedObjectsLimitException extends IllegalArgumentException {
    public ListedObjectsLimitException(int listedCount, int maxListedObjects) {
        super(
            LoggerMessageFormat.format(
                "Glob pattern listed too many objects ({}, limit {}). Narrow your glob pattern, add partition "
                    + "filters, or increase the [esql.external.max_listed_objects] cluster setting.",
                listedCount,
                maxListedObjects
            )
        );
    }
}
