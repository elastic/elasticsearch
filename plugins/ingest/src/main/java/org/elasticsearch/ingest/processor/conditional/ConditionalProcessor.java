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

package org.elasticsearch.ingest.processor.conditional;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.io.IOException;
import java.util.*;

public final class ConditionalProcessor implements Processor {

    public static final String TYPE = "conditional";

    private final ConditionalExpression conditionalExpression;
    private final List<Processor> matchedProcessors;
    private final List<Processor> unmatchedProcessors;

    ConditionalProcessor(ConditionalExpression conditionalExpression, List<Processor> matchedProcessors, List<Processor> unmatchedProcessors) {
        this.conditionalExpression = conditionalExpression;
        this.matchedProcessors = matchedProcessors;
        this.unmatchedProcessors = unmatchedProcessors;
    }

    ConditionalExpression getConditionalExpression() {
        return conditionalExpression;
    }

    List<Processor> getMatchedProcessors() {
        return matchedProcessors;
    }

    List<Processor> getUnmatchedProcessors() {
        return unmatchedProcessors;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        if (conditionalExpression.matches(ingestDocument)) {
            for (Processor processor : matchedProcessors) {
                processor.execute(ingestDocument);
            }
        } else {
            for (Processor processor : unmatchedProcessors) {
                processor.execute(ingestDocument);
            }
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory<ConditionalProcessor> {
        private Map<String, Processor.Factory> processorRegistry;

        @Override
        public void setProcessorRegistry(Map<String, Processor.Factory> processorRegistry) {
            this.processorRegistry = processorRegistry;
        }

        private RelationalExpression readRelationalExpression(Map<String, Map<String, Object>> conf) {
            if (conf != null && conf.size() == 1) {
                String field = ((Map.Entry<String, Object>) conf.entrySet().toArray()[0]).getKey();
                Map.Entry<String, Object> firstAndOnlyExpr = (Map.Entry<String, Object>) conf.get(field).entrySet().toArray()[0];
                return new RelationalExpression(field, firstAndOnlyExpr.getKey(), firstAndOnlyExpr.getValue());
            } else {
                throw new IllegalArgumentException("only one relation expression allowed per field condition.");
            }
        }

        @Override
        public ConditionalProcessor create(Map<String, Object> config) throws Exception {
            Map<String, Object> conditionsConfig = ConfigurationUtils.readMap(config, "if");
            List<Map<String, Map<String, Object>>> matchProcessorsList = ConfigurationUtils.readList(config, "then");
            List<Map<String, Map<String, Object>>> otherwiseProcessorsList = ConfigurationUtils.readOptionalList(config, "else");

            if (conditionsConfig.size() != 1) {
                throw new IllegalArgumentException("only one expression allowed.");
            }

            ConditionalExpression conditionalExpression = null;

            Map.Entry<String, Object> entry = (Map.Entry<String, Object>) conditionsConfig.entrySet().toArray()[0];
            if (("all".equals(entry.getKey()) || "any".equals(entry.getKey())) && (entry.getValue() instanceof List)) {
                List<RelationalExpression> relationalExpressions = new ArrayList<>();
                for (Map<String, Map<String, Object>> conf : (List<Map<String, Map<String, Object>>>) entry.getValue()) {
                    RelationalExpression e = readRelationalExpression(conf);
                    relationalExpressions.add(e);
                }
                conditionalExpression = new BooleanExpression(entry.getKey(), relationalExpressions);
            } else {
                conditionalExpression = readRelationalExpression(Collections.singletonMap(entry.getKey(), (Map<String, Object>) entry.getValue()));
            }

            List<Processor> matchProcessors = ConfigurationUtils.readProcessors(matchProcessorsList, processorRegistry);
            List<Processor> otherwiseProcessors = ConfigurationUtils.readProcessors(otherwiseProcessorsList, processorRegistry);

            return new ConditionalProcessor(conditionalExpression, matchProcessors, otherwiseProcessors);
        }
    }
}
