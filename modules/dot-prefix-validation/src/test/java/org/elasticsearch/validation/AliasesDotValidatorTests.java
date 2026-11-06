/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.validation;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The dot-prefix rule for the new name of an index that is renamed through the aliases API.
 */
public class AliasesDotValidatorTests extends ESTestCase {

    private static final SystemIndices SYSTEM_INDICES = new SystemIndices(
        List.of(
            new SystemIndices.Feature(
                "test",
                "test system indices",
                List.of(
                    SystemIndexDescriptor.builder()
                        .setIndexPattern(".test-system-index*")
                        .setDescription("test system index")
                        .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
                        .setOrigin("test")
                        .build()
                )
            )
        )
    );

    private static AliasesDotValidator validator(boolean stateless) {
        final Settings.Builder settings = Settings.builder();
        if (stateless) {
            settings.put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true);
        }
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings.build(),
                Sets.newHashSet(DotPrefixValidator.VALIDATE_DOT_PREFIXES, DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING)
            )
        );
        when(clusterService.getSettings()).thenReturn(settings.build());
        when(clusterService.threadPool()).thenReturn(mock(ThreadPool.class));
        return new AliasesDotValidator(new ThreadContext(Settings.EMPTY), clusterService, SYSTEM_INDICES);
    }

    private static IndicesAliasesRequest request(IndicesAliasesRequest.AliasActions... actions) {
        final IndicesAliasesRequest request = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        for (IndicesAliasesRequest.AliasActions action : actions) {
            request.addAliasAction(action);
        }
        return request;
    }

    public void testActionName() {
        assertThat(validator(randomBoolean()).actionName(), equalTo(TransportIndicesAliasesAction.NAME));
    }

    /** Only the new name of a rename is an index name that the request creates. */
    public void testOnlyTheNewNameOfARenameIsChecked() {
        final IndicesAliasesRequest request = request(
            IndicesAliasesRequest.AliasActions.add().index(".added-to").alias(".alias"),
            IndicesAliasesRequest.AliasActions.remove().index(".removed-from").alias(".other-alias"),
            IndicesAliasesRequest.AliasActions.removeIndex().index(".removed"),
            IndicesAliasesRequest.AliasActions.renameIndex().index(".old-name").newIndex("new-name"),
            IndicesAliasesRequest.AliasActions.renameIndex().index("plain").newIndex(".new-dot-name")
        );
        assertThat(validator(randomBoolean()).getIndicesFromRequest(request), equalTo(Set.of("new-name", ".new-dot-name")));
    }

    public void testRequestWithoutARenameChecksNothing() {
        final IndicesAliasesRequest request = request(
            IndicesAliasesRequest.AliasActions.add().index(".index").alias(".alias"),
            IndicesAliasesRequest.AliasActions.removeIndex().index(".gone")
        );
        assertThat(validator(randomBoolean()).getIndicesFromRequest(request), empty());
    }

    public void testStatelessRefusesADotPrefixedNewName() {
        final AliasesDotValidator validator = validator(true);
        final IndicesAliasesRequest dotted = request(IndicesAliasesRequest.AliasActions.renameIndex().index("source").newIndex(".target"));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateIndices(validator.getIndicesFromRequest(dotted))
        );
        assertThat(e.getMessage(), containsString("Index [.target] name beginning with a dot (.) is not allowed"));

        // the same skips as for creating an index
        validator.validateIndices(
            validator.getIndicesFromRequest(
                request(IndicesAliasesRequest.AliasActions.renameIndex().index("source").newIndex(".test-system-index-1"))
            )
        );
        validator.validateIndices(
            validator.getIndicesFromRequest(
                request(IndicesAliasesRequest.AliasActions.renameIndex().index("source").newIndex(".ml-state-000001"))
            )
        );
        validator.validateIndices(
            validator.getIndicesFromRequest(request(IndicesAliasesRequest.AliasActions.renameIndex().index(".source").newIndex("target")))
        );
    }
}
