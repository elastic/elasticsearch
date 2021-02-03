/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.lucene.util.SetOnce;

import java.util.Arrays;

/**
 * Converts {@code %node_name} in log4j patterns into the current node name.
 * We can't use a system property for this because the node name system
 * property is only set if the node name is explicitly defined in
 * elasticsearch.yml.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "NodeNamePatternConverter")
@ConverterKeys({"ESnode_name","node_name"})
public final class NodeNamePatternConverter extends LogEventPatternConverter {
    /**
     * The name of this node.
     */
    private static final SetOnce<String> NODE_NAME = new SetOnce<>();

    /**
     * Set the name of this node.
     */
    static void setNodeName(String nodeName) {
        NODE_NAME.set(nodeName);
    }

    public static void setGlobalNodeName(String nodeName){
        LoggerContext ctx = LoggerContext.getContext(false);
        ctx.getConfiguration().getProperties().put("node_name",nodeName);
    }

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static NodeNamePatternConverter newInstance(final String[] options) {
        if (options.length > 0) {
            throw new IllegalArgumentException("no options supported but options provided: "
                    + Arrays.toString(options));
        }
        String nodeName = NODE_NAME.get();
        if (nodeName == null) {
            throw new IllegalStateException("the node name hasn't been set");
        }
        return new NodeNamePatternConverter(nodeName);
    }

    private final String nodeName;

    private NodeNamePatternConverter(String nodeName) {
        super("NodeName", "node_name");
        this.nodeName = nodeName;
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        toAppendTo.append(nodeName);
    }
}
