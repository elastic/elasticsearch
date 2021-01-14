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
package org.elasticsearch.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Locale;

import org.elasticsearch.client.textstructure.FindStructureRequest;
import org.elasticsearch.client.textstructure.FindStructureResponse;
import org.elasticsearch.client.textstructure.structurefinder.TextStructure;

public class TextStructureIT extends ESRestHighLevelClientTestCase {

    public void testFindFileStructure() throws IOException {

        String sample = "{\"logger\":\"controller\",\"timestamp\":1478261151445,\"level\":\"INFO\"," +
                "\"pid\":42,\"thread\":\"0x7fff7d2a8000\",\"message\":\"message 1\",\"class\":\"ml\"," +
                "\"method\":\"core::SomeNoiseMaker\",\"file\":\"Noisemaker.cc\",\"line\":333}\n" +
            "{\"logger\":\"controller\",\"timestamp\":1478261151445," +
                "\"level\":\"INFO\",\"pid\":42,\"thread\":\"0x7fff7d2a8000\",\"message\":\"message 2\",\"class\":\"ml\"," +
                "\"method\":\"core::SomeNoiseMaker\",\"file\":\"Noisemaker.cc\",\"line\":333}\n";

        TextStructureClient textStructureClient = highLevelClient().textStructure();

        FindStructureRequest request = new FindStructureRequest();
        request.setSample(sample.getBytes(StandardCharsets.UTF_8));

        FindStructureResponse response = execute(
            request,
            textStructureClient::findStructure,
            textStructureClient::findStructureAsync,
            RequestOptions.DEFAULT);

        TextStructure structure = response.getFileStructure();

        assertEquals(2, structure.getNumLinesAnalyzed());
        assertEquals(2, structure.getNumMessagesAnalyzed());
        assertEquals(sample, structure.getSampleStart());
        assertEquals(TextStructure.Format.NDJSON, structure.getFormat());
        assertEquals(StandardCharsets.UTF_8.displayName(Locale.ROOT), structure.getCharset());
        assertFalse(structure.getHasByteOrderMarker());
        assertNull(structure.getMultilineStartPattern());
        assertNull(structure.getExcludeLinesPattern());
        assertNull(structure.getColumnNames());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getShouldTrimFields());
        assertNull(structure.getGrokPattern());
        assertEquals(Collections.singletonList("UNIX_MS"), structure.getJavaTimestampFormats());
        assertEquals(Collections.singletonList("UNIX_MS"), structure.getJodaTimestampFormats());
        assertEquals("timestamp", structure.getTimestampField());
        assertFalse(structure.needClientTimezone());
    }

}
