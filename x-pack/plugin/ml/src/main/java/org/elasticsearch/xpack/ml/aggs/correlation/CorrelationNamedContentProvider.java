/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.List;

import static java.util.Collections.singletonList;

public final class CorrelationNamedContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return singletonList(
            new NamedXContentRegistry.Entry(
                CorrelationFunction.class,
                CountCorrelationFunction.NAME,
                CountCorrelationFunction::fromXContent
            )
        );
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return singletonList(
            new NamedWriteableRegistry.Entry(
                CorrelationFunction.class,
                CountCorrelationFunction.NAME.getPreferredName(),
                CountCorrelationFunction::new
            )
        );
    }
}
