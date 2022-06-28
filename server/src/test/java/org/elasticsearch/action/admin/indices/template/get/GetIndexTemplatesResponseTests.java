/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.hamcrest.Matchers.equalTo;

public class GetIndexTemplatesResponseTests extends AbstractXContentTestCase<GetIndexTemplatesResponse> {
    @Override
    protected GetIndexTemplatesResponse doParseInstance(XContentParser parser) throws IOException {
        return GetIndexTemplatesResponse.fromXContent(parser);
    }

    @Override
    protected GetIndexTemplatesResponse createTestInstance() {
        List<IndexTemplateMetadata> templates = new ArrayList<>();
        int numTemplates = between(0, 10);
        for (int t = 0; t < numTemplates; t++) {
            IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder("template-" + t);
            templateBuilder.patterns(IntStream.range(0, between(1, 5)).mapToObj(i -> "pattern-" + i).collect(Collectors.toList()));
            int numAlias = between(0, 5);
            for (int i = 0; i < numAlias; i++) {
                templateBuilder.putAlias(AliasMetadata.builder(randomAlphaOfLengthBetween(1, 10)));
            }
            if (randomBoolean()) {
                templateBuilder.settings(Settings.builder().put("index.setting-1", randomLong()));
            }
            if (randomBoolean()) {
                templateBuilder.order(randomInt());
            }
            if (randomBoolean()) {
                templateBuilder.version(between(0, 100));
            }
            if (randomBoolean()) {
                try {
                    templateBuilder.putMapping("doc", "{\"doc\":{\"properties\":{\"type\":\"text\"}}}");
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
            templates.add(templateBuilder.build());
        }
        return new GetIndexTemplatesResponse(templates);
    }

    @Override
    protected boolean supportsUnknownFields() {
        // We can not inject anything at the top level because a GetIndexTemplatesResponse is serialized as a map
        // from template name to template content. IndexTemplateMetadataTests already covers situations where we
        // inject arbitrary things inside the IndexTemplateMetadata.
        return false;
    }

    /**
     * For now, we only unit test the legacy typed responses. This will soon no longer be the case,
     * as we introduce support for typeless xContent parsing in {@link GetFieldMappingsResponse}.
     */
    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(INCLUDE_TYPE_NAME_PARAMETER, "true"));
    }

    @Override
    protected void assertEqualInstances(GetIndexTemplatesResponse expectedInstance, GetIndexTemplatesResponse newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertThat(new HashSet<>(newInstance.getIndexTemplates()), equalTo(new HashSet<>(expectedInstance.getIndexTemplates())));
    }
}
