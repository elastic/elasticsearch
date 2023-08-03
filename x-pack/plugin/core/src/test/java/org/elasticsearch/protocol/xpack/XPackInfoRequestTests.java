/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.protocol.xpack;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.protocol.xpack.XPackInfoRequest.Category;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class XPackInfoRequestTests extends ESTestCase {

    public void testSerializeToCurrentVersion() throws Exception {
        assertSerialization(TransportVersion.current());
    }

    public void testSerializeUsing7xVersion() throws Exception {
        assertSerialization(
            TransportVersionUtils.randomVersionBetween(
                random(),
                TransportVersion.V_7_8_1,
                TransportVersionUtils.getPreviousVersion(TransportVersion.V_8_0_0)
            )
        );
    }

    private void assertSerialization(TransportVersion version) throws java.io.IOException {
        final XPackInfoRequest request = new XPackInfoRequest();
        final List<Category> categories = randomSubsetOf(List.of(Category.values()));
        request.setCategories(categories.isEmpty() ? EnumSet.noneOf(Category.class) : EnumSet.copyOf(categories));
        request.setVerbose(randomBoolean());
        final XPackInfoRequest read = copyWriteable(request, new NamedWriteableRegistry(List.of()), XPackInfoRequest::new, version);
        assertThat(
            "Serialized request with version [" + version + "] does not match original [categories]",
            read.getCategories(),
            equalTo(request.getCategories())
        );
        assertThat(
            "Serialized request with version [" + version + "] does not match original [verbose]",
            read.isVerbose(),
            equalTo(request.isVerbose())
        );
    }

}
