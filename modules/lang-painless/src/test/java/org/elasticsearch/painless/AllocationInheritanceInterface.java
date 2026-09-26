/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

/**
 * Support interface for the allocation annotation-inheritance tests: its methods are the ones carrying the allocation
 * annotations, while {@link AllocationInheritanceObject} allowlists the same methods <em>without</em> annotations. A def call
 * resolves to the (unannotated) implementation method, so charging it exercises the walk that finds the annotated interface
 * method.
 */
public interface AllocationInheritanceInterface {

    int inheritedConstant();

    int inheritedDynamic(int n);
}
