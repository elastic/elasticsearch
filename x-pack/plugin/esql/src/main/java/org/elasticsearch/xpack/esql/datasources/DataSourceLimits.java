/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.view.ViewService;

/** Fixed bounds on user-authored fields of data sources and datasets, which are stored in cluster state. */
public final class DataSourceLimits {
    /**
     * Maximum length of a data source or dataset {@code description}, in UTF-16 code units. Tied to
     * {@link ViewService#MAX_VIEW_DESCRIPTION_LENGTH} so all ES|QL catalog objects share one description cap.
     */
    public static final int MAX_DESCRIPTION_LENGTH = ViewService.MAX_VIEW_DESCRIPTION_LENGTH;

    private DataSourceLimits() {}
}
