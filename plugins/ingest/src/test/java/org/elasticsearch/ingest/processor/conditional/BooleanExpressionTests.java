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
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BooleanExpressionTests extends ESTestCase {

    public void testMatchesAll() {
        IngestDocument doc = new IngestDocument("index", "type", "id", Collections.EMPTY_MAP);
        RelationalExpression e1 = mock(RelationalExpression.class);
        RelationalExpression e2 = mock(RelationalExpression.class);
        when(e1.matches(doc)).thenReturn(true);
        when(e2.matches(doc)).thenReturn(true);
        BooleanExpression expression = new BooleanExpression(BooleanExpression.BooleanOperator.ALL, Arrays.asList(e1, e2));
        assertTrue(expression.matches(doc));
    }

    public void testNotMatchesAll() {
        IngestDocument doc = new IngestDocument("index", "type", "id", Collections.EMPTY_MAP);
        RelationalExpression e1 = mock(RelationalExpression.class);
        RelationalExpression e2 = mock(RelationalExpression.class);
        when(e1.matches(doc)).thenReturn(false);
        when(e2.matches(doc)).thenReturn(true);
        BooleanExpression expression = new BooleanExpression(BooleanExpression.BooleanOperator.ALL, Arrays.asList(e1, e2));
        assertFalse(expression.matches(doc));
    }

    public void testMatchesAny() {
        IngestDocument doc = new IngestDocument("index", "type", "id", Collections.EMPTY_MAP);
        RelationalExpression e1 = mock(RelationalExpression.class);
        RelationalExpression e2 = mock(RelationalExpression.class);
        when(e1.matches(doc)).thenReturn(true);
        when(e2.matches(doc)).thenReturn(false);
        BooleanExpression expression = new BooleanExpression(BooleanExpression.BooleanOperator.ANY, Arrays.asList(e1, e2));
        assertTrue(expression.matches(doc));
    }

    public void testNotMatchesAny() {
        IngestDocument doc = new IngestDocument("index", "type", "id", Collections.EMPTY_MAP);
        RelationalExpression e1 = mock(RelationalExpression.class);
        RelationalExpression e2 = mock(RelationalExpression.class);
        when(e1.matches(doc)).thenReturn(false);
        when(e2.matches(doc)).thenReturn(false);
        BooleanExpression expression = new BooleanExpression(BooleanExpression.BooleanOperator.ANY, Arrays.asList(e1, e2));
        assertFalse(expression.matches(doc));
    }
}
