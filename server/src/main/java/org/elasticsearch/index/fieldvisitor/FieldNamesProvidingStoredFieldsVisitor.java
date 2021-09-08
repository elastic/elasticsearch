/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.StoredFieldVisitor;

import java.util.Set;

/**
 * Stored fields visitor which provides information about the field names that will be requested
 */
public abstract class FieldNamesProvidingStoredFieldsVisitor extends StoredFieldVisitor {
    public abstract Set<String> getFieldNames();
}
