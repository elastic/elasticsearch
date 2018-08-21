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

/**
 * Converts {@code %node_name} in log4j patterns into the current node name.
 * We can't use a system property for this because the node name system
 * property is only set if the node name is explicitly defined in
 * elasticsearch.yml.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "NodeNamePatternConverter")
@ConverterKeys({"node_name"})
public final class NodeNamePatternConverter extends LogEventPatternConverter {
    /**
     * The name of this node. This is intentionally not {@code volatile} so we
     * don't have to pay for the volatile read when we log every log line.
     * Instead we rely on a volatile inside of log4j2 to set up the
     * "happens before" relationship guaranteeing the correctness here.
     * Specifically, before every log, log4j2 reads
     * {@code Logger.privateConfiguration}. These volatiles are written to
     * after every write to this static variable. The only two writes to this
     * variable are both followed shortly by writes to
     * {@code Logger.privateConfiguration}. Thus all writes all writes to this
     * variable "happen before" all reads from this variable. So long as we
     * don't add any more writes. And we don't plan to. We also guard against
     * it by declaring the variable private and making the method to write to
     * it package private. It isn't great protection, but we plan to remove
     * the need for all of this shortly.
     */
    private static String nodeName = "unknown";

    /**
     * Set the name of this node.
     */
    static void setNodeName(String nodeName) {
        NodeNamePatternConverter.nodeName = nodeName;
    }

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static NodeNamePatternConverter newInstance(final String[] options) {
        if (options.length > 0) {
            throw new IllegalArgumentException("no options supported but options provided: "
                    + Arrays.toString(options));
        }
        return new NodeNamePatternConverter();
    }

    private NodeNamePatternConverter() {
        super("NodeName", "node_name");
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        toAppendTo.append(nodeName);
    }
}
