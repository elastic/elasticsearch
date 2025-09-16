/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;

public class SamplingPlugin extends Plugin {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(SamplingMetadata.TYPE),
                SamplingMetadata::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                SamplingConfiguration.class,
                new ParseField(SamplingConfiguration.TYPE),
                SamplingConfiguration::fromXContent
            )
        );
    }

}
