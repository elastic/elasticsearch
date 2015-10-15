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

package org.elasticsearch.search.fetch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsParseElement;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.junit.Test;

public class FieldDataFieldsTests extends ESTestCase {

    public void testValidFieldDataFieldString() throws Exception {
        FieldDataFieldsParseElement parseElement = new FieldDataFieldsParseElement();

        BytesArray data = new BytesArray(new BytesRef("{\"fielddata_fields\": \"foobar\"}"));
        XContentParser parser = XContentFactory.xContent(data).createParser(data);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();
        SearchContext context = new TestSearchContext();
        parseElement.parse(parser, context);
    }

    public void testValidFieldDataFieldArray() throws Exception {
        FieldDataFieldsParseElement parseElement = new FieldDataFieldsParseElement();

        BytesArray data = new BytesArray(new BytesRef("{\"fielddata_fields\": [ \"foo\", \"bar\", \"baz\"]}}"));
        XContentParser parser = XContentFactory.xContent(data).createParser(data);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();
        SearchContext context = new TestSearchContext();
        parseElement.parse(parser, context);
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidFieldDataField() throws Exception {
        FieldDataFieldsParseElement parseElement = new FieldDataFieldsParseElement();

        BytesArray data;
        if (randomBoolean()) {
            data = new BytesArray(new BytesRef("{\"fielddata_fields\": {}}}"));
        } else {
            data = new BytesArray(new BytesRef("{\"fielddata_fields\": 1.0}}"));
        }
        XContentParser parser = XContentFactory.xContent(data).createParser(data);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();
        SearchContext context = new TestSearchContext();
        parseElement.parse(parser, context);
    }
}
