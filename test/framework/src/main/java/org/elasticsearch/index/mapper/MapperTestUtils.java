/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;

import java.io.IOException;

import static org.apache.lucene.util.LuceneTestCase.expectThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class MapperTestUtils {

    public static void assertConflicts(String mapping1, String mapping2, DocumentMapperParser parser, String... conflicts) throws IOException {
        DocumentMapper docMapper = parser.parse("type", new CompressedXContent(mapping1));
        if (conflicts.length == 0) {
            docMapper.merge(parser.parse("type", new CompressedXContent(mapping2)).mapping(), MapperService.MergeReason.MAPPING_UPDATE);
        } else {
            Exception e = expectThrows(IllegalArgumentException.class,
                () -> docMapper.merge(parser.parse("type", new CompressedXContent(mapping2)).mapping(), MapperService.MergeReason.MAPPING_UPDATE));
            for (String conflict : conflicts) {
                assertThat(e.getMessage(), containsString(conflict));
            }
        }
    }

}
