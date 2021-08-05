/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.client.transform.transforms.RetentionPolicyConfig;
import org.elasticsearch.client.transform.transforms.SyncConfig;
import org.elasticsearch.client.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.client.transform.transforms.TimeSyncConfig;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;

import java.util.Arrays;
import java.util.List;

public class TransformNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            new NamedXContentRegistry.Entry(SyncConfig.class, new ParseField(TimeSyncConfig.NAME), TimeSyncConfig::fromXContent),
            new NamedXContentRegistry.Entry(
                RetentionPolicyConfig.class,
                new ParseField(TimeRetentionPolicyConfig.NAME),
                TimeRetentionPolicyConfig::fromXContent
            )
        );
    }

}
