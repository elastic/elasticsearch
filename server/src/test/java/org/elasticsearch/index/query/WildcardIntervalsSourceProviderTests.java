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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.index.query.IntervalsSourceProvider.Wildcard;

public class WildcardIntervalsSourceProviderTests extends AbstractSerializingTestCase<Wildcard> {

    @Override
    protected Wildcard createTestInstance() {
        return createRandomWildcard();
    }

    static Wildcard createRandomWildcard() {
        return new Wildcard(
            randomAlphaOfLength(10),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }

    @Override
    protected Wildcard mutateInstance(Wildcard instance) throws IOException {
        String wildcard = instance.getPattern();
        String analyzer = instance.getAnalyzer();
        String useField = instance.getUseField();
        switch (between(0, 2)) {
            case 0:
                wildcard += "a";
                break;
            case 1:
                analyzer = randomAlphaOfLength(5);
                break;
            case 2:
                useField = useField == null ? randomAlphaOfLength(5) : null;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new Wildcard(wildcard, analyzer, useField);
    }

    @Override
    protected Writeable.Reader<Wildcard> instanceReader() {
        return Wildcard::new;
    }

    @Override
    protected Wildcard doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Wildcard wildcard = (Wildcard) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return wildcard;
    }
}
