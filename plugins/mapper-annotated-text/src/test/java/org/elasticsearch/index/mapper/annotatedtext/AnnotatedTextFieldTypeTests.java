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

package org.elasticsearch.index.mapper.annotatedtext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Collections;

public class AnnotatedTextFieldTypeTests extends FieldTypeTestCase {

    public void testIntervals() throws IOException {
        MappedFieldType ft = new AnnotatedTextFieldMapper.AnnotatedTextFieldType("field", Collections.emptyMap());
        NamedAnalyzer a = new NamedAnalyzer("name", AnalyzerScope.INDEX, new StandardAnalyzer());
        IntervalsSource source = ft.intervals("Donald Trump", 0, true, a, false);
        assertEquals(Intervals.phrase(Intervals.term("donald"), Intervals.term("trump")), source);
    }
}
