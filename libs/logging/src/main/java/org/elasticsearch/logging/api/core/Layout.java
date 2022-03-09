/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.api.core;

import co.elastic.logging.log4j2.EcsLayout;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.elasticsearch.logging.internal.ECSJsonLayout;
import org.elasticsearch.logging.internal.EcsLayoutImpl;

import java.io.Serializable;

public interface Layout {

    static Layout createECSLayout(String dataset) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            final Configuration config = ctx.getConfiguration();

            EcsLayout layout =    ECSJsonLayout.newBuilder()
                        .setDataset(dataset)
                        .setConfiguration(config)
                        .build();

        return new EcsLayoutImpl(layout);
    }

    byte[] toByteArray(LogEvent event);
}
