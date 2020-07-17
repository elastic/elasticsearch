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
package org.elasticsearch.index.mapper;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Base test case for subclasses of MappedFieldType */
public abstract class FieldTypeTestCase extends ESTestCase {

    public static final QueryShardContext MOCK_QSC = createMockQueryShardContext(true);
    public static final QueryShardContext MOCK_QSC_DISALLOW_EXPENSIVE = createMockQueryShardContext(false);

    protected QueryShardContext randomMockShardContext() {
        return randomFrom(MOCK_QSC, MOCK_QSC_DISALLOW_EXPENSIVE);
    }

    static QueryShardContext createMockQueryShardContext(boolean allowExpensiveQueries) {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.allowExpensiveQueries()).thenReturn(allowExpensiveQueries);
        return queryShardContext;
    }

}
