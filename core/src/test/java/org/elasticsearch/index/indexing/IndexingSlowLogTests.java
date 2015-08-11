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

package org.elasticsearch.index.indexing;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.indexing.IndexingSlowLog.SlowLogParsedDocumentPrinter;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class IndexingSlowLogTests extends ESTestCase {
    public void testSlowLogParsedDocumentPrinterSourceToLog() throws IOException {
        BytesReference source = JsonXContent.contentBuilder().startObject().field("foo", "bar").endObject().bytes();
        ParsedDocument pd = new ParsedDocument(new StringField("uid", "test:id", Store.YES), new IntField("version", 1, Store.YES), "id",
                "test", null, 0, -1, null, source, null);

        // Turning off document logging doesn't log source[]
        SlowLogParsedDocumentPrinter p = new SlowLogParsedDocumentPrinter(pd, 10, true, 0);
        assertThat(p.toString(), not(containsString("source[")));

        // Turning on document logging logs the whole thing
        p = new SlowLogParsedDocumentPrinter(pd, 10, true, Integer.MAX_VALUE);
        assertThat(p.toString(), containsString("source[{\"foo\":\"bar\"}]"));

        // And you can truncate the source
        p = new SlowLogParsedDocumentPrinter(pd, 10, true, 3);
        assertThat(p.toString(), containsString("source[{\"f]"));
    }
}
