/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.source.field;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.source.SourceProvider;
import org.elasticsearch.index.source.SourceProviderParser;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;

/**
 *
 */
public class DefaultSourceProviderParser implements SourceProviderParser {

    @Override public SourceProvider parse(Map<String, Object> node) {
        DefaultSourceProvider.Builder builder = new DefaultSourceProvider.Builder();
        for (Map.Entry<String, Object> entry : node.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("compress") && fieldNode != null) {
                builder.compress(nodeBooleanValue(fieldNode));
            } else if (fieldName.equals("compress_threshold") && fieldNode != null) {
                if (fieldNode instanceof Number) {
                    builder.compressThreshold(((Number) fieldNode).longValue());
                    builder.compress(true);
                } else {
                    builder.compressThreshold(ByteSizeValue.parseBytesSizeValue(fieldNode.toString()).bytes());
                    builder.compress(true);
                }
            } else if ("format".equals(fieldName)) {
                builder.format(nodeStringValue(fieldNode, null));
            } else if (fieldName.equals("includes")) {
                List<Object> values = (List<Object>) fieldNode;
                String[] includes = new String[values.size()];
                for (int i = 0; i < includes.length; i++) {
                    includes[i] = values.get(i).toString();
                }
                builder.includes(includes);
            } else if (fieldName.equals("excludes")) {
                List<Object> values = (List<Object>) fieldNode;
                String[] excludes = new String[values.size()];
                for (int i = 0; i < excludes.length; i++) {
                    excludes[i] = values.get(i).toString();
                }
                builder.excludes(excludes);
            }
        }

        return builder.build();
    }
}
