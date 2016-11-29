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

package org.elasticsearch.mapper.attachments;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.junit.Before;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class WrongAttachmentMapperTests extends AttachmentUnitTestCase {

    private DocumentMapperParser mapperParser;

    @Before
    public void setupMapperParser() throws Exception {
        mapperParser = MapperTestUtils.newMapperService(createTempDir(), Settings.EMPTY,
            getIndicesModuleWithRegisteredAttachmentMapper()).documentMapperParser();

    }

    public void testSimpleMappings() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/unit/wrong-mapping/wrong-mapping.json");

        try {
            mapperParser.parse("person", new CompressedXContent(mapping));
            fail("We should have raised a MapperParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), is("Type [nonexistingtype] is not supported. Check your [content] field."));
        }
    }
}
