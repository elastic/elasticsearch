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

package org.elasticsearch.index.query;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.IntervalsSourceProvider.Fuzzy;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class FuzzyIntervalsSourceProviderTests extends AbstractSerializingTestCase<Fuzzy> {
    @Override
    protected Fuzzy createTestInstance() {
        return new Fuzzy(
            randomAlphaOfLength(10),
            randomInt(5),
            randomBoolean(),
            Fuzziness.fromEdits(randomInt(2)),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    @Override
    protected Fuzzy mutateInstance(Fuzzy instance) throws IOException {
        String term = instance.getTerm();
        int prefixLength = instance.getPrefixLength();
        boolean isTranspositions = instance.isTranspositions();
        Fuzziness fuzziness = instance.getFuzziness();
        String analyzer = instance.getAnalyzer();
        String useField = instance.getUseField();
        switch (between(0, 5)) {
            case 0:
                term = randomAlphaOfLength(5);
                break;
            case 1:
                prefixLength++;
                break;
            case 2:
                isTranspositions = !isTranspositions;
                break;
            case 3:
                if (fuzziness.equals(Fuzziness.ZERO)) {
                    fuzziness = Fuzziness.ONE;
                } else {
                    fuzziness = Fuzziness.ZERO;
                }
                break;
            case 4:
                analyzer = analyzer == null ? randomAlphaOfLength(5) : null;
                break;
            case 5:
                useField = useField == null ? randomAlphaOfLength(5) : null;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new Fuzzy(
            term,
            prefixLength,
            isTranspositions,
            fuzziness,
            analyzer,
            useField
        );
    }

    @Override
    protected Writeable.Reader<Fuzzy> instanceReader() {
        return Fuzzy::new;
    }

    @Override
    protected Fuzzy doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Fuzzy Fuzzy = (Fuzzy) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return Fuzzy;
    }
}
