/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ToXContentFragment;

/**
 * Represents a metric colleged as part of {@link ExtendedSearchUsageStats}.
 */
public interface ExtendedSearchUsageMetric<T extends ExtendedSearchUsageMetric<T>> extends NamedWriteable, ToXContentFragment {

    /**
     * Merges two equivalent metrics together for statistical reporting.
     * @param other Another {@link ExtendedSearchUsageMetric}.
     * @return ExtendedSearchUsageMetric The merged metric.
     */
    T merge(ExtendedSearchUsageMetric<?> other);
}
