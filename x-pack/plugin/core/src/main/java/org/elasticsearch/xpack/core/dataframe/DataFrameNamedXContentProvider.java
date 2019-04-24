/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xpack.core.dataframe.transforms.SyncConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.TimeSyncConfig;

import java.util.Arrays;
import java.util.List;

public class DataFrameNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
                new NamedXContentRegistry.Entry(SyncConfig.class,
                        DataFrameField.TIME_BASED_SYNC,
                        TimeSyncConfig::parse));
    }
}
