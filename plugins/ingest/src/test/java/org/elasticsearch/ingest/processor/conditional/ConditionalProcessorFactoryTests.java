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

package org.elasticsearch.ingest.processor.conditional;

import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConditionalProcessorFactoryTests extends ESTestCase {
    ConditionalProcessor.Factory factory;
    Map<String, Object> config;
    List<Map<String, Object>> matchedProcessors;
    List<Map<String, Object>> unmatchedProcessors;
    Processor anyProcessor;

    @Before
    public void init() throws Exception {
        config = new HashMap<>();
        factory = new ConditionalProcessor.Factory();

        Map<String, Object> processorConfig = Collections.singletonMap("some_processor", Collections.EMPTY_MAP);
        matchedProcessors = Collections.singletonList(processorConfig);
        unmatchedProcessors = Collections.singletonList(processorConfig);

        Processor.Factory processorFactory = mock(Processor.Factory.class);
        anyProcessor = mock(Processor.class);
        when(processorFactory.create(anyMap())).thenReturn(anyProcessor);
        Map<String, Processor.Factory> processorRegistry = new HashMap<>();
        processorRegistry.put("some_processor", processorFactory);
        factory.setProcessorRegistry(processorRegistry);
    }

    public void testSingleRelationalExpression() throws Exception {
        config.put("if", Collections.singletonMap("foo", Collections.singletonMap("gt", 10)));
        config.put("then", matchedProcessors);
        config.put("else", unmatchedProcessors);
        ConditionalProcessor conditionalProcessor = factory.create(config);
        RelationalExpression expectedConditionalExpression = new RelationalExpression("foo", "gt", 10);
        List<Processor> expectedMatchedProcessors = Collections.singletonList(anyProcessor);
        List<Processor> expectedUnmatchedProcessors = Collections.singletonList(anyProcessor);

        RelationalExpression expr = (RelationalExpression) conditionalProcessor.getConditionalExpression();
        assertThat(expr.getOperand(), equalTo(expectedConditionalExpression.getOperand()));
        assertThat(expr.getOperator(), equalTo(expectedConditionalExpression.getOperator()));
        assertThat(conditionalProcessor.getMatchedProcessors(), equalTo(expectedMatchedProcessors));
        assertThat(conditionalProcessor.getUnmatchedProcessors(), equalTo(expectedUnmatchedProcessors));
    }

    public void testAllExpression() throws Exception {
        config.put("if", Collections.singletonMap("all",
                Arrays.asList(Collections.singletonMap("foo", Collections.singletonMap("gt", 10)),
                        Collections.singletonMap("foo", Collections.singletonMap("lt", 20)))));
        config.put("then", matchedProcessors);
        config.put("else", unmatchedProcessors);

        ConditionalProcessor conditionalProcessor = factory.create(config);

        RelationalExpression gtExpr = new RelationalExpression("foo", "gt", 10);
        RelationalExpression ltExpr = new RelationalExpression("foo", "lt", 20);
        List<Processor> expectedMatchedProcessors = Collections.singletonList(anyProcessor);
        List<Processor> expectedUnmatchedProcessors = Collections.singletonList(anyProcessor);

        BooleanExpression expr = (BooleanExpression) conditionalProcessor.getConditionalExpression();
        assertThat(expr.getBooleanOperator(), equalTo(BooleanExpression.BooleanOperator.ALL));
        assertThat(expr.getRelationalExpressions().get(0).getOperator(), equalTo(gtExpr.getOperator()));
        assertThat(expr.getRelationalExpressions().get(0).getOperand(), equalTo(gtExpr.getOperand()));
        assertThat(expr.getRelationalExpressions().get(1).getOperator(), equalTo(ltExpr.getOperator()));
        assertThat(expr.getRelationalExpressions().get(1).getOperand(), equalTo(ltExpr.getOperand()));
        assertThat(conditionalProcessor.getMatchedProcessors(), equalTo(expectedMatchedProcessors));
        assertThat(conditionalProcessor.getUnmatchedProcessors(), equalTo(expectedUnmatchedProcessors));
    }

    public void testAnyExpression() throws Exception {
        config.put("if", Collections.singletonMap("any",
                Arrays.asList(Collections.singletonMap("foo", Collections.singletonMap("gt", 10)),
                        Collections.singletonMap("foo", Collections.singletonMap("lt", 20)))));
        config.put("then", matchedProcessors);
        config.put("else", unmatchedProcessors);

        ConditionalProcessor conditionalProcessor = factory.create(config);

        RelationalExpression gtExpr = new RelationalExpression("foo", "gt", 10);
        RelationalExpression ltExpr = new RelationalExpression("foo", "lt", 20);
        List<Processor> expectedMatchedProcessors = Collections.singletonList(anyProcessor);
        List<Processor> expectedUnmatchedProcessors = Collections.singletonList(anyProcessor);

        BooleanExpression expr = (BooleanExpression) conditionalProcessor.getConditionalExpression();
        assertThat(expr.getBooleanOperator(), equalTo(BooleanExpression.BooleanOperator.ANY));
        assertThat(expr.getRelationalExpressions().get(0).getOperator(), equalTo(gtExpr.getOperator()));
        assertThat(expr.getRelationalExpressions().get(0).getOperand(), equalTo(gtExpr.getOperand()));
        assertThat(expr.getRelationalExpressions().get(1).getOperator(), equalTo(ltExpr.getOperator()));
        assertThat(expr.getRelationalExpressions().get(1).getOperand(), equalTo(ltExpr.getOperand()));
        assertThat(conditionalProcessor.getMatchedProcessors(), equalTo(expectedMatchedProcessors));
        assertThat(conditionalProcessor.getUnmatchedProcessors(), equalTo(expectedUnmatchedProcessors));
    }
}
