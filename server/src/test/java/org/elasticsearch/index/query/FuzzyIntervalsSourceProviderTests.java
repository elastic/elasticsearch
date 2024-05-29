/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.IntervalsSourceProvider.Fuzzy;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class FuzzyIntervalsSourceProviderTests extends AbstractXContentSerializingTestCase<Fuzzy> {
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
    protected Fuzzy mutateInstance(Fuzzy instance) {
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
                isTranspositions = isTranspositions == false;
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
        return new Fuzzy(term, prefixLength, isTranspositions, fuzziness, analyzer, useField);
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
