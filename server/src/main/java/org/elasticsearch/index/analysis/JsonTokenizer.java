/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.index.analysis;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.util.AttributeFactory;

import java.io.IOException;
import java.io.Reader;

public final class JsonTokenizer extends Tokenizer {
    private static final AttributeFactory ATTRIBUTE_FACTORY = AttributeFactory.getStaticImplementation(
        AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY,
        CharTermAttributeImpl.class);
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private final CharTermAttribute termAtt;
    private JsonParser jsonParser;

    JsonTokenizer() {
        super(ATTRIBUTE_FACTORY);
        termAtt = addAttribute(CharTermAttribute.class);
        jsonParser = createParser(input);
    }

    private static JsonParser createParser(Reader input) {
        try {
            return JSON_FACTORY.createParser(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean incrementToken() throws IOException {
        clearAttributes();

        while (true) {
            JsonToken token = jsonParser.nextToken();
            if (token == null) {
                return false;
            }

            if (token.isScalarValue()) {
                termAtt.append(jsonParser.getValueAsString());
                return true;
            }
        }
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        jsonParser.close();
        jsonParser = JSON_FACTORY.createParser(input);
    }

    @Override
    public void close() throws IOException {
        super.close();
        jsonParser.close();
    }
}
