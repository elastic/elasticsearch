/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.lucene.util.SetOnce;

import java.util.Arrays;

/**
 * Converts {@code %cluster_name} in log4j patterns into the current cluster name.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "ClusterNamePatternConverter")
@ConverterKeys({ "EScluster_name", "cluster_name" })
public final class ClusterNamePatternConverter extends LogEventPatternConverter {
    /**
     * The name of this cluster.
     */
    private static final SetOnce<String> CLUSTER_NAME = new SetOnce<>();

    /**
     * Set the name of this cluster.
     */
    static void setClusterName(String clusterName) {
        CLUSTER_NAME.set(clusterName);
    }

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static ClusterNamePatternConverter newInstance(final String[] options) {
        if (options.length > 0) {
            throw new IllegalArgumentException("no options supported but options provided: " + Arrays.toString(options));
        }
        String clusterName = CLUSTER_NAME.get();
        if (clusterName == null) {
            throw new IllegalStateException("the cluster name hasn't been set");
        }
        return new ClusterNamePatternConverter(clusterName);
    }

    private final String clusterName;

    private ClusterNamePatternConverter(String clusterName) {
        super("ClusterName", "cluster_name");
        this.clusterName = clusterName;
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        toAppendTo.append(clusterName);
    }
}
