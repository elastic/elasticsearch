/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public abstract class AbstractPrefilteredQueryTestCase<QB extends AbstractQueryBuilder<QB> & PrefilteredQuery<QB>> extends
    AbstractQueryTestCase<QB> {

    public void testSerializationPrefiltersBwc() throws Exception {
        QB originalQuery = createTestQueryBuilder();
        originalQuery.setPrefilters(randomList(1, 5, () -> RandomQueryBuilder.createQuery(random())));

        for (int i = 0; i < 100; i++) {
            TransportVersion transportVersion = TransportVersionUtils.randomVersionBetween(
                random(),
                originalQuery.getMinimalSupportedVersion().id() == 0
                    ? TransportVersions.V_8_0_0 // The first major before introducing prefiltering
                    : originalQuery.getMinimalSupportedVersion(),
                TransportVersionUtils.getPreviousVersion(TransportVersion.current())
            );

            @SuppressWarnings("unchecked")
            QB deserializedQuery = (QB) copyNamedWriteable(originalQuery, namedWriteableRegistry(), QueryBuilder.class, transportVersion);

            if (transportVersion.supports(PrefilteredQuery.QUERY_PREFILTERING)) {
                assertThat(deserializedQuery, equalTo(originalQuery));
            } else {
                QB originalQueryWithoutPrefilters = copyQuery(originalQuery).setPrefilters(List.of());
                assertThat(deserializedQuery, equalTo(originalQueryWithoutPrefilters));
            }
        }
    }

    public void testEqualsAndHashcodeForPrefilters() throws IOException {
        QB originalQuery = createTestQueryBuilder();
        originalQuery.setPrefilters(randomList(1, 5, () -> RandomQueryBuilder.createQuery(random())));

        @SuppressWarnings("unchecked")
        QB deserializedQuery = (QB) copyNamedWriteable(originalQuery, namedWriteableRegistry(), QueryBuilder.class);

        assertThat(deserializedQuery, equalTo(originalQuery));
        assertThat(deserializedQuery.hashCode(), equalTo(originalQuery.hashCode()));

        deserializedQuery.setPrefilters(List.of());
        assertThat(deserializedQuery, not(equalTo(originalQuery)));
        assertThat(deserializedQuery.hashCode(), not(equalTo(originalQuery.hashCode())));
    }

}
