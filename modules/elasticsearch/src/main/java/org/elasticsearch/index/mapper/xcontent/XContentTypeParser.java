/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.mapper.xcontent;

import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.gcommon.collect.ImmutableMap;

import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public interface XContentTypeParser {

    public static class ParserContext {

        private final AnalysisService analysisService;

        private final Map<String, Object> rootNode;

        private final ImmutableMap<String, XContentTypeParser> typeParsers;

        public ParserContext(Map<String, Object> rootNode, AnalysisService analysisService, ImmutableMap<String, XContentTypeParser> typeParsers) {
            this.analysisService = analysisService;
            this.rootNode = rootNode;
            this.typeParsers = typeParsers;
        }

        public AnalysisService analysisService() {
            return analysisService;
        }

        public Map<String, Object> rootNode() {
            return this.rootNode;
        }

        public XContentTypeParser typeParser(String type) {
            return typeParsers.get(Strings.toUnderscoreCase(type));
        }
    }

    XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException;
}
