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

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IndexTemplateV2MetadataTests extends AbstractNamedWriteableTestCase<IndexTemplateV2Metadata> {
    @Override
    protected IndexTemplateV2Metadata createTestInstance() {
        if (randomBoolean()) {
            return new IndexTemplateV2Metadata(Collections.emptyMap());
        }
        Map<String, IndexTemplateV2> templates = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            templates.put(randomAlphaOfLength(5), IndexTemplateV2Tests.randomInstance());
        }
        return new IndexTemplateV2Metadata(templates);
    }

    @Override
    protected IndexTemplateV2Metadata mutateInstance(IndexTemplateV2Metadata instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.singletonList(new NamedWriteableRegistry.Entry(IndexTemplateV2Metadata.class,
            IndexTemplateV2Metadata.TYPE, IndexTemplateV2Metadata::new)));
    }

    @Override
    protected Class<IndexTemplateV2Metadata> categoryClass() {
        return IndexTemplateV2Metadata.class;
    }
}
