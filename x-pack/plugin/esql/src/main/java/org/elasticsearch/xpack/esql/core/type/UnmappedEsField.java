/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

/**
 * An {@link EsField} standing in for a field this index does not map, rather than one read from the mapping.
 */
public interface UnmappedEsField {}
