/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.index.IndexAuditTrailField;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.index.IndexNameResolver;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;

import java.util.function.Predicate;

public class XPackUserTests extends ESTestCase {

    public void testXPackUserCanAccessNonSecurityIndices() {
        final String action = randomFrom(GetAction.NAME, SearchAction.NAME, IndexAction.NAME);
        final Predicate<String> predicate = XPackUser.ROLE.indices().allowedIndicesMatcher(action);
        final String index = randomBoolean() ? randomAlphaOfLengthBetween(3, 12) : "." + randomAlphaOfLength(8);
        assertThat(predicate.test(index), Matchers.is(true));
    }

    public void testXPackUserCannotAccessRestrictedIndices() {
        final String action = randomFrom(GetAction.NAME, SearchAction.NAME, IndexAction.NAME);
        final Predicate<String> predicate = XPackUser.ROLE.indices().allowedIndicesMatcher(action);
        for (String index : RestrictedIndicesNames.RESTRICTED_NAMES) {
            assertThat(predicate.test(index), Matchers.is(false));
        }
    }

    public void testXPackUserCanReadAuditTrail() {
        final String action = randomFrom(GetAction.NAME, SearchAction.NAME);
        final Predicate<String> predicate = XPackUser.ROLE.indices().allowedIndicesMatcher(action);
        assertThat(predicate.test(getAuditLogName()), Matchers.is(true));
    }

    public void testXPackUserCannotWriteToAuditTrail() {
        final String action = randomFrom(IndexAction.NAME, UpdateAction.NAME);
        final Predicate<String> predicate = XPackUser.ROLE.indices().allowedIndicesMatcher(action);
        assertThat(predicate.test(getAuditLogName()), Matchers.is(false));
    }

    private String getAuditLogName() {
        final DateTime date = new DateTime().plusDays(randomIntBetween(1, 360));
        final IndexNameResolver.Rollover rollover = randomFrom(IndexNameResolver.Rollover.values());
        return IndexNameResolver.resolve(IndexAuditTrailField.INDEX_NAME_PREFIX, date, rollover);
    }
}
