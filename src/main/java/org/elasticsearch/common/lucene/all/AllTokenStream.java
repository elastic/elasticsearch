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

package org.elasticsearch.common.lucene.all;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

import static org.apache.lucene.analysis.payloads.PayloadHelper.encodeFloat;

/**
 *
 */
public final class AllTokenStream extends TokenFilter {

    public static TokenStream allTokenStream(String allFieldName, AllEntries allEntries, Analyzer analyzer) throws IOException {
        return new AllTokenStream(analyzer.tokenStream(allFieldName, allEntries), allEntries);
    }
    
    private final BytesRef payloadSpare = new BytesRef(new byte[4]);

    private final AllEntries allEntries;

    private final OffsetAttribute offsetAttribute;
    private final PayloadAttribute payloadAttribute;

    AllTokenStream(TokenStream input, AllEntries allEntries) {
        super(input);
        this.allEntries = allEntries;
        offsetAttribute = addAttribute(OffsetAttribute.class);
        payloadAttribute = addAttribute(PayloadAttribute.class);
    }

    public AllEntries allEntries() {
        return allEntries;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (!input.incrementToken()) {
            return false;
        }
        final float boost = allEntries.boost(offsetAttribute.startOffset());
        if (boost != 1.0f) {
            encodeFloat(boost, payloadSpare.bytes, payloadSpare.offset);
            payloadAttribute.setPayload(payloadSpare);
        } else {
            payloadAttribute.setPayload(null);
        }
        return true;
    }

    @Override
    public String toString() {
        return allEntries.toString();
    }
}
