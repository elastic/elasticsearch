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

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.test.ESTestCase;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class RelationalExpressionTests extends ESTestCase {
    IngestDocument ingestDocumentWithIntFoo = new IngestDocument("index", "type", "id", Collections.singletonMap("foo", 10));
    IngestDocument ingestDocumentWithStringFoo = new IngestDocument("index", "type", "id", Collections.singletonMap("foo", "bar"));

    public void testGte() {
        RelationalExpression expression = new RelationalExpression("foo", "gte", 9);
        expression.matches(ingestDocumentWithIntFoo);
        assertTrue(expression.matches(ingestDocumentWithIntFoo));
    }

    public void testLte() {
        RelationalExpression expression = new RelationalExpression("foo", "lte", 13);
        expression.matches(ingestDocumentWithIntFoo);
        assertTrue(expression.matches(ingestDocumentWithIntFoo));
    }

    public void testLt() {
        RelationalExpression expression = new RelationalExpression("foo", "lt", 13);
        expression.matches(ingestDocumentWithIntFoo);
        assertTrue(expression.matches(ingestDocumentWithIntFoo));
    }

    public void testGt() {
        RelationalExpression expression = new RelationalExpression("foo", "gt", 8);
        expression.matches(ingestDocumentWithIntFoo);
        assertTrue(expression.matches(ingestDocumentWithIntFoo));
    }

    public void testNumericComparisonWithoutNumberInDocument() {
        RelationalExpression expression = new RelationalExpression("foo", "gt", 8);
        assertFalse(expression.matches(ingestDocumentWithStringFoo));
    }

    public void testNumericComparisonWithoutNumberInOperand() {
        RelationalExpression expression = new RelationalExpression("foo", "gt", "foo");
        assertFalse(expression.matches(ingestDocumentWithIntFoo));
    }

    public void testEq() {
        RelationalExpression expression = new RelationalExpression("foo", "eq", "bar");
        expression.matches(ingestDocumentWithStringFoo);
        assertTrue(expression.matches(ingestDocumentWithStringFoo));
    }

    public void testNeq() {
        RelationalExpression expression = new RelationalExpression("foo", "neq", "hello");
        expression.matches(ingestDocumentWithStringFoo);
        assertTrue(expression.matches(ingestDocumentWithStringFoo));
    }
}
