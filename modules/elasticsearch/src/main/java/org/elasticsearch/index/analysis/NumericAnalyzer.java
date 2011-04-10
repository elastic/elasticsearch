/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;
import java.io.Reader;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class NumericAnalyzer<T extends NumericTokenizer> extends Analyzer {

    @Override public final TokenStream tokenStream(String fieldName, Reader reader) {
        try {
            return createNumericTokenizer(reader, new char[32]);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create numeric tokenizer", e);
        }
    }

    @Override public final TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
        Holder holder = (Holder) getPreviousTokenStream();
        if (holder == null) {
            char[] buffer = new char[120];
            holder = new Holder(createNumericTokenizer(reader, buffer), buffer);
            setPreviousTokenStream(holder);
        } else {
            holder.tokenizer.reset(reader, holder.buffer);
        }
        return holder.tokenizer;
    }

    protected abstract T createNumericTokenizer(Reader reader, char[] buffer) throws IOException;

    private static final class Holder {
        final NumericTokenizer tokenizer;
        final char[] buffer;

        private Holder(NumericTokenizer tokenizer, char[] buffer) {
            this.tokenizer = tokenizer;
            this.buffer = buffer;
        }
    }
}
