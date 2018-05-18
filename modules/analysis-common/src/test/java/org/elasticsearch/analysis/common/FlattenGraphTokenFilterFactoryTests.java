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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;

public class FlattenGraphTokenFilterFactoryTests extends ESTokenStreamTestCase {

    public void testBasic() throws IOException {

        Index index = new Index("test", "_na_");
        String name = "ngr";
        Settings indexSettings = newAnalysisSettingsBuilder().build();
        IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, indexSettings);
        Settings settings = newAnalysisSettingsBuilder().build();

        // "wow that's funny" and "what the fudge" are separate side paths, in parallel with "wtf", on input:
        TokenStream in = new CannedTokenStream(0, 12, new Token[] {
                    token("wtf", 1, 5, 0, 3),
                    token("what", 0, 1, 0, 3),
                    token("wow", 0, 3, 0, 3),
                    token("the", 1, 1, 0, 3),
                    token("fudge", 1, 3, 0, 3),
                    token("that's", 1, 1, 0, 3),
                    token("funny", 1, 1, 0, 3),
                    token("happened", 1, 1, 4, 12)
                });

        TokenStream tokens = new FlattenGraphTokenFilterFactory(indexProperties, null, name, settings).create(in);

        // ... but on output, it's flattened to wtf/what/wow that's/the fudge/funny happened:
        assertTokenStreamContents(tokens,
                new String[] {"wtf", "what", "wow", "the", "that's", "fudge", "funny", "happened"},
                new int[] {0, 0, 0, 0, 0, 0, 0, 4},
                new int[] {3, 3, 3, 3, 3, 3, 3, 12},
                new int[] {1, 0, 0, 1, 0, 1, 0, 1},
                new int[] {3, 1, 1, 1, 1, 1, 1, 1},
                12);
    }

    private static Token token(String term, int posInc, int posLength, int startOffset, int endOffset) {
        final Token t = new Token(term, startOffset, endOffset);
        t.setPositionIncrement(posInc);
        t.setPositionLength(posLength);
        return t;
    }
}
