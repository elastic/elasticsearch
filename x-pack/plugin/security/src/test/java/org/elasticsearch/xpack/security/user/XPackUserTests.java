/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.index.IndexAuditTrailField;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.index.IndexNameResolver;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.function.Predicate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XPackUserTests extends ESTestCase {

    public void testXPackUserCanAccessNonSecurityIndices() {
        for (String action : Arrays.asList(GetAction.NAME, DeleteAction.NAME, SearchAction.NAME, IndexAction.NAME)) {
            Predicate<IndexAbstraction> predicate = XPackUser.ROLE.indices().allowedIndicesMatcher(action);
            IndexAbstraction index = mockIndexAbstraction(randomAlphaOfLengthBetween(3, 12));
            if (false == RestrictedIndicesNames.isRestricted(index.getName())) {
                assertThat(predicate.test(index), Matchers.is(true));
            }
            index = mockIndexAbstraction("." + randomAlphaOfLengthBetween(3, 12));
            if (false == RestrictedIndicesNames.isRestricted(index.getName())) {
                assertThat(predicate.test(index), Matchers.is(true));
            }
        }
    }

    public void testXPackUserCannotAccessRestrictedIndices() {
        for (String action : Arrays.asList(GetAction.NAME, DeleteAction.NAME, SearchAction.NAME, IndexAction.NAME)) {
            Predicate<IndexAbstraction> predicate = XPackUser.ROLE.indices().allowedIndicesMatcher(action);
            for (String index : RestrictedIndicesNames.RESTRICTED_NAMES) {
                assertThat(predicate.test(mockIndexAbstraction(index)), Matchers.is(false));
            }
            assertThat(predicate.test(mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))),
                    Matchers.is(false));
        }
    }

    public void testXPackUserCanReadAuditTrail() {
        final String action = randomFrom(GetAction.NAME, SearchAction.NAME);
        final Predicate<IndexAbstraction> predicate = XPackUser.ROLE.indices().allowedIndicesMatcher(action);
        assertThat(predicate.test(mockIndexAbstraction(getAuditLogName())), Matchers.is(true));
    }

    public void testXPackUserCannotWriteToAuditTrail() {
        final String action = randomFrom(IndexAction.NAME, UpdateAction.NAME);
        final Predicate<IndexAbstraction> predicate = XPackUser.ROLE.indices().allowedIndicesMatcher(action);
        assertThat(predicate.test(mockIndexAbstraction(getAuditLogName())), Matchers.is(false));
    }

    private String getAuditLogName() {
        final DateTime date = new DateTime().plusDays(randomIntBetween(1, 360));
        final IndexNameResolver.Rollover rollover = randomFrom(IndexNameResolver.Rollover.values());
        return IndexNameResolver.resolve(IndexAuditTrailField.INDEX_NAME_PREFIX, date, rollover);
    }

    private IndexAbstraction mockIndexAbstraction(String name) {
        IndexAbstraction mock = mock(IndexAbstraction.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(randomFrom(IndexAbstraction.Type.CONCRETE_INDEX,
                IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM));
        return mock;
    }
}
