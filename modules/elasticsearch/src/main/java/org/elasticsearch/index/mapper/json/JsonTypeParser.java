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

package org.elasticsearch.index.mapper.json;

import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.util.Strings;

/**
 * @author kimchy (shay.banon)
 */
public interface JsonTypeParser {

    public static class ParserContext {

        private final AnalysisService analysisService;

        private final ObjectNode rootNode;

        private final ImmutableMap<String, JsonTypeParser> typeParsers;

        public ParserContext(ObjectNode rootNode, AnalysisService analysisService, ImmutableMap<String, JsonTypeParser> typeParsers) {
            this.analysisService = analysisService;
            this.rootNode = rootNode;
            this.typeParsers = typeParsers;
        }

        public AnalysisService analysisService() {
            return analysisService;
        }

        public ObjectNode rootNode() {
            return this.rootNode;
        }

        public JsonTypeParser typeParser(String type) {
            return typeParsers.get(Strings.toUnderscoreCase(type));
        }
    }

    JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException;
}
