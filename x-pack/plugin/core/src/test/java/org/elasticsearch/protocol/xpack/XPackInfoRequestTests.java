/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.protocol.xpack;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.protocol.xpack.XPackInfoRequest.Category;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class XPackInfoRequestTests extends ESTestCase {

    public void testSerializeToCurrentVersion() throws Exception {
        assertSerialization(Version.CURRENT);
    }

    public void testSerializeUsing7xVersion() throws Exception {
        // At the time of writing, V7.8.1 is unreleased, so there's no easy way to use VersionUtils to get a random version between
        // 7.8.1 (inclusive) and 8.0.0 (exclusive), because the "version before 8.0.0" returns 7.8.0 (the most recent released version).
        // To work around this we accept that 8.0.0 is included in the range, and then filter it out using other-than
        final Version version = randomValueOtherThan(
            Version.V_8_0_0,
            () -> VersionUtils.randomVersionBetween(random(), Version.V_7_8_1, Version.V_8_0_0)
        );
        assertSerialization(version);
    }

    private void assertSerialization(Version version) throws java.io.IOException {
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
