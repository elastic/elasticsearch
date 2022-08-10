/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.util.List;

public final class ChangePointNamedContentProvider {

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(ChangeType.class, ChangeType.StepChange.NAME, ChangeType.StepChange::new),
            new NamedWriteableRegistry.Entry(ChangeType.class, ChangeType.Spike.NAME, ChangeType.Spike::new),
            new NamedWriteableRegistry.Entry(ChangeType.class, ChangeType.TrendChange.NAME, ChangeType.TrendChange::new),
            new NamedWriteableRegistry.Entry(ChangeType.class, ChangeType.Dip.NAME, ChangeType.Dip::new),
            new NamedWriteableRegistry.Entry(ChangeType.class, ChangeType.DistributionChange.NAME, ChangeType.DistributionChange::new),
            new NamedWriteableRegistry.Entry(ChangeType.class, ChangeType.Stationary.NAME, ChangeType.Stationary::new),
            new NamedWriteableRegistry.Entry(ChangeType.class, ChangeType.NonStationary.NAME, ChangeType.NonStationary::new),
            new NamedWriteableRegistry.Entry(ChangeType.class, ChangeType.Indeterminable.NAME, ChangeType.Indeterminable::new)
        );
    }
}
