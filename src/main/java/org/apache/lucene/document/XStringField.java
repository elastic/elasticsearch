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
package org.apache.lucene.document;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.CloseableThreadLocal;

import java.io.IOException;

/**
 * A string/text field that optimizes the case for non analyzed fields to reuse a thread local token
 * stream (instead of creating it each time). This reduces analysis chain overhead and object creation
 * (which is significant, yay Attributes).
 * <p/>
 * Not to be confused with Lucene StringField, this handles analyzed text as well, and relies on providing
 * the FieldType. Couldn't come up with a good name for this that is different from Text/String...
 */
public class XStringField extends Field {

    private static final CloseableThreadLocal<StringTokenStream> NOT_ANALYZED_TOKENSTREAM = new CloseableThreadLocal<StringTokenStream>() {
        @Override
        protected StringTokenStream initialValue() {
            return new StringTokenStream();
        }
    };

    public XStringField(String name, String value, FieldType fieldType) {
        super(name, fieldType);
        fieldsData = value;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer) throws IOException {
        if (!fieldType().indexed()) {
            return null;
        }
        // Only use the cached TokenStream if the value is indexed and not-tokenized
        if (fieldType().tokenized()) {
            return super.tokenStream(analyzer);
        }
        StringTokenStream nonAnalyzedTokenStream = NOT_ANALYZED_TOKENSTREAM.get();
        nonAnalyzedTokenStream.setValue((String) fieldsData);
        return nonAnalyzedTokenStream;
    }
}
