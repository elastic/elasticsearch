/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import java.util.Arrays;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.lucene.util.SetOnce;

/**
 * Converts {@code %node_name} in log4j patterns into the current node name.
 * We *could* use a system property lookup instead but this is very explicit
 * and fails fast if we try to use the logger without initializing the node
 * name. As a bonus it ought to be ever so slightly faster because it doesn't
 * have to look up the system property every time.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "NodeNamePatternConverter")
@ConverterKeys({"node_name"})
public class NodeNamePatternConverter extends LogEventPatternConverter {
    private static final SetOnce<String> NODE_NAME = new SetOnce<>();

    /**
     * Set the name of this node.
     */
    public static void setNodeName(String nodeName) {
        NODE_NAME.set(nodeName);
    }

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static NodeNamePatternConverter newInstance(final String[] options) {
        if (options.length > 0) {
            throw new IllegalArgumentException("no options supported but options provided: "
                    + Arrays.toString(options));
        }
        return new NodeNamePatternConverter(NODE_NAME.get());
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
