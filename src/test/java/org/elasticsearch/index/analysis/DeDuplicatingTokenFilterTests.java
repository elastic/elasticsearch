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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Test;

import java.io.IOException;

public class DeDuplicatingTokenFilterTests extends ElasticsearchTokenStreamTestCase {

    @Test
    public void testSimpleWrappingStreamNoDups() throws IOException {
        int bufferSize = 10000;
        ByteStreamDuplicateSequenceSpotter spotter = new ByteStreamDuplicateSequenceSpotter(bufferSize);

        testAdditions(spotter, 3, "this is a test", new String[] { "this", "is", "a", "test" });
        testAdditions(spotter, 3, "and this is a test too", new String[] { "and", "too" });
        testAdditions(spotter, 3, "this is ok", new String[] { "this", "is", "ok" });
        testAdditions(spotter, 5, "this is a test", new String[] { "this", "is", "a", "test" });
        testAdditions(spotter, 4, "this is a test", new String[] {});
        // This call checks that the prior doc "this is a test" is seen as distinct from the new doc that starts with "too"
        testAdditions(spotter, 3, "too new", new String[] { "too", "new" });

    }

    private void testAdditions(ByteStreamDuplicateSequenceSpotter spotter, int dupLength, String input, String[] expectedOutputs)
            throws IOException {
        WhitespaceAnalyzer ws = new WhitespaceAnalyzer(Version.LUCENE_4_9);
        TokenStream stream = ws.tokenStream("default", new FastStringReader(input));
        stream = new DeDuplicatingTokenFilter(Version.LUCENE_4_9, stream, spotter, dupLength);
        assertTokenStreamContents(stream, expectedOutputs);
    }

}
