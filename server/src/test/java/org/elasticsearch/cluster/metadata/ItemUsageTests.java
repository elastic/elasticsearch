/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ItemUsageTests extends AbstractWireTestCase<ItemUsage> {

    public static ItemUsage randomUsage() {
        return new ItemUsage(randomStringList(), randomStringList(), randomStringList());
    }

    @Nullable
    private static List<String> randomStringList() {
        if (randomBoolean()) {
            return null;
        } else {
            return randomList(0, 1, () -> randomAlphaOfLengthBetween(2, 10));
        }
    }

    @Override
    protected ItemUsage createTestInstance() {
        return randomUsage();
    }

    @Override
    protected ItemUsage copyInstance(ItemUsage instance, TransportVersion version) throws IOException {
        return new ItemUsage(instance.getIndices(), instance.getDataStreams(), instance.getComposableTemplates());
    }

    @Override
    protected ItemUsage mutateInstance(ItemUsage instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    // We test this in a unit test and not a yml test because it is not required by API,
    // but it has been existing behavior that we chose to preserve.
    public void testEmptyInstanceXContent() throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            String json = Strings.toString(ItemUsage.EMPTY.toXContent(builder, ToXContent.EMPTY_PARAMS));
            assertThat(json, containsString("\"indices\":[]"));
            assertThat(json, containsString("\"data_streams\":[]"));
            assertThat(json, containsString("\"composable_templates\":[]"));
        }
    }
}
