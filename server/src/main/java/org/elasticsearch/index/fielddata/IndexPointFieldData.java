/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.geo.SpatialPoint;

/**
 * Specialization of {@link IndexFieldData} for geo points and points.
 */
public interface IndexPointFieldData<T extends MultiPointValues<? extends SpatialPoint>> extends IndexFieldData<LeafPointFieldData<T>> {}
