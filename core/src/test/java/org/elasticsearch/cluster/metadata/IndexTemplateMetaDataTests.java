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
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

import static org.elasticsearch.cluster.metadata.AliasMetaData.newAliasMetaDataBuilder;

public class IndexTemplateMetaDataTests extends ESTestCase {

    // bwc for #21009
    public void testIndexTemplateMetaData510() throws IOException {
        IndexTemplateMetaData metaData = IndexTemplateMetaData.builder("foo")
            .patterns(Collections.singletonList("bar"))
            .order(1)
            .settings(Settings.builder()
                .put("setting1", "value1")
                .put("setting2", "value2"))
            .putAlias(newAliasMetaDataBuilder("alias-bar1")).build();

        IndexTemplateMetaData multiMetaData = IndexTemplateMetaData.builder("foo")
            .patterns(Arrays.asList("bar", "foo"))
            .order(1)
            .settings(Settings.builder()
                .put("setting1", "value1")
                .put("setting2", "value2"))
            .putAlias(newAliasMetaDataBuilder("alias-bar1")).build();

        // These bytes were retrieved by Base64 encoding the result of the above with 5_0_0 code
        String templateBytes = "A2ZvbwAAAAEDYmFyAghzZXR0aW5nMQEGdmFsdWUxCHNldHRpbmcyAQZ2YWx1ZTIAAQphbGlhcy1iYXIxAAAAAAA=";
        BytesArray bytes = new BytesArray(Base64.getDecoder().decode(templateBytes));

        try (StreamInput in = bytes.streamInput()) {
            in.setVersion(Version.V_5_0_0);
            IndexTemplateMetaData readMetaData = IndexTemplateMetaData.readFrom(in);
            assertEquals(0, in.available());
            assertEquals(metaData.getName(), readMetaData.getName());
            assertEquals(metaData.getPatterns(), readMetaData.getPatterns());
            assertTrue(metaData.aliases().containsKey("alias-bar1"));
            assertEquals(1, metaData.aliases().size());

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.V_5_0_0);
            readMetaData.writeTo(output);
            assertEquals(bytes.toBytesRef(), output.bytes().toBytesRef());

            // test that multi templates are reverse-compatible.
            // for the bwc case, if multiple patterns, use only the first pattern seen.
            output.reset();
            multiMetaData.writeTo(output);
            assertEquals(bytes.toBytesRef(), output.bytes().toBytesRef());
        }
    }

}
