/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class UpgradeTransformsResponseTests extends ESTestCase {

    public void testXContentParser() throws IOException {
        xContentTester(
            this::createParser,
            UpgradeTransformsResponseTests::createTestInstance,
            UpgradeTransformsResponseTests::toXContent,
            UpgradeTransformsResponse::fromXContent
        ).assertToXContentEquivalence(false).supportsUnknownFields(false).test();
    }

    private static UpgradeTransformsResponse createTestInstance() {
        return new UpgradeTransformsResponse(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    private static void toXContent(UpgradeTransformsResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("updated", response.getUpdated());
        builder.field("no_action", response.getNoAction());
        builder.field("needs_update", response.getNeedsUpdate());
        builder.endObject();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.addAll(new TransformNamedXContentProvider().getNamedXContentParsers());

        return new NamedXContentRegistry(namedXContents);
    }

}
