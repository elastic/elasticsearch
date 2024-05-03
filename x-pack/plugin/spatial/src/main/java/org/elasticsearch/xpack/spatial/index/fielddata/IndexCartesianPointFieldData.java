/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexPointFieldData;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSource;

/**
 * Specialization of {@link IndexFieldData} for cartesian points.
 */
public interface IndexCartesianPointFieldData extends IndexPointFieldData<CartesianPointValuesSource.MultiCartesianPointValues> {}
