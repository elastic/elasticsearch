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
package org.elasticsearch.common;

import com.google.common.base.Charsets;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.is;

/**
 *
 */
public class Base64Test extends ElasticsearchTestCase {

    @Test // issue #6334
    public void testBase64DecodeWithExtraCharactersAfterPadding() throws Exception {
        String plain = randomAsciiOfLengthBetween(1, 20) + ":" + randomAsciiOfLengthBetween(1, 20);
        String encoded = Base64.encodeBytes(plain.getBytes(Charsets.UTF_8));
        assertValidBase64(encoded, plain);

        // lets append some trash here, if the encoded string has been padded
        char lastChar = encoded.charAt(encoded.length() - 1);
        if (lastChar == '=') {
            assertInvalidBase64(encoded + randomAsciiOfLength(3));
        }
    }

    private void assertValidBase64(String base64, String expected) throws IOException {
        String decoded = new String(Base64.decode(base64.getBytes(Charsets.UTF_8)), Charsets.UTF_8);
        assertThat(decoded, is(expected));
    }

    private void assertInvalidBase64(String base64) {
        try {
            Base64.decode(base64.getBytes(Charsets.UTF_8));
            fail(String.format(Locale.ROOT, "Expected IOException to be thrown for string %s (len %d)", base64, base64.length()));
        } catch (IOException e) {}
    }
}
