/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DlsFlsRequestCacheDifferentiatorTests extends ESTestCase {

    private MockLicenseState licenseState;
    private ThreadContext threadContext;
    private StreamOutput out;
    private DlsFlsRequestCacheDifferentiator differentiator;
    private ShardSearchRequest shardSearchRequest;
    private String indexName;
    private String dlsIndexName;
    private String flsIndexName;
    private String dlsFlsIndexName;

    @Before
    public void init() throws IOException {
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        threadContext = new ThreadContext(Settings.EMPTY);
        out = new BytesStreamOutput();
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        differentiator = new DlsFlsRequestCacheDifferentiator(
            licenseState,
            new SetOnce<>(securityContext),
            new SetOnce<>(mock(ScriptService.class))
        );
        shardSearchRequest = mock(ShardSearchRequest.class);
        indexName = randomAlphaOfLengthBetween(3, 8);
        dlsIndexName = "dls-" + randomAlphaOfLengthBetween(3, 8);
        flsIndexName = "fls-" + randomAlphaOfLengthBetween(3, 8);
        dlsFlsIndexName = "dls-fls-" + randomAlphaOfLengthBetween(3, 8);

        final DocumentPermissions documentPermissions1 = DocumentPermissions.filteredBy(Set.of(new BytesArray("""
            {"term":{"number":1}}""")));

        securityContext.putIndicesAccessControl(
            new IndicesAccessControl(
                true,
                Map.of(
                    flsIndexName,
                    new IndicesAccessControl.IndexAccessControl(
                        new FieldPermissions(new FieldPermissionsDefinition(new String[] { "*" }, new String[] { "private" })),
                        DocumentPermissions.allowAll()
                    ),
                    dlsIndexName,
                    new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, documentPermissions1),
                    dlsFlsIndexName,
                    new IndicesAccessControl.IndexAccessControl(
                        new FieldPermissions(new FieldPermissionsDefinition(new String[] { "*" }, new String[] { "private" })),
                        documentPermissions1
                    )
                )
            )
        );
    }

    public void testWillWriteCacheKeyForAnyDlsOrFls() throws IOException {
        when(shardSearchRequest.shardId()).thenReturn(
            new ShardId(randomFrom(dlsIndexName, flsIndexName, dlsFlsIndexName), randomAlphaOfLength(10), randomIntBetween(0, 3))
        );
        differentiator.accept(shardSearchRequest, out);
        assertThat(out.position(), greaterThan(0L));
    }

    public void testWillDoNothingIfNoDlsFls() throws IOException {
        when(shardSearchRequest.shardId()).thenReturn(new ShardId(indexName, randomAlphaOfLength(10), randomIntBetween(0, 3)));
        differentiator.accept(shardSearchRequest, out);
        assertThat(out.position(), equalTo(0L));
    }

    private DlsFlsRequestCacheDifferentiator differentiatorWithCurrentName(String currentName) {
        return new DlsFlsRequestCacheDifferentiator(
            licenseState,
            new SetOnce<>(new SecurityContext(Settings.EMPTY, threadContext)),
            new SetOnce<>(mock(ScriptService.class)),
            shardId -> currentName
        );
    }

    /**
     * A request routed before a rename carries the old name of the index, while the access control holds the name the data node
     * resolved, which is the current one when the request went through an alias. The key has to describe the restrictions anyway.
     */
    public void testWillWriteCacheKeyUnderTheCurrentNameOfARenamedIndex() throws IOException {
        when(shardSearchRequest.shardId()).thenReturn(new ShardId("old-name", randomAlphaOfLength(10), randomIntBetween(0, 3)));
        differentiatorWithCurrentName(randomFrom(dlsIndexName, flsIndexName, dlsFlsIndexName)).accept(shardSearchRequest, out);
        assertThat(out.position(), greaterThan(0L));
    }

    /** The key is the same whichever of the two names the access control has the entry under. */
    public void testCacheKeyIsTheSameUnderEitherNameOfARenamedIndex() throws IOException {
        final String name = randomFrom(dlsIndexName, flsIndexName, dlsFlsIndexName);
        final StreamOutput byCurrentName = new BytesStreamOutput();
        when(shardSearchRequest.shardId()).thenReturn(new ShardId("old-name", randomAlphaOfLength(10), 0));
        differentiatorWithCurrentName(name).accept(shardSearchRequest, byCurrentName);

        final StreamOutput byRequestedName = new BytesStreamOutput();
        when(shardSearchRequest.shardId()).thenReturn(new ShardId(name, randomAlphaOfLength(10), 0));
        differentiatorWithCurrentName(name).accept(shardSearchRequest, byRequestedName);

        assertThat(byCurrentName.position(), greaterThan(0L));
        assertThat(byCurrentName.position(), equalTo(byRequestedName.position()));
    }

    /** With an entry under neither name, a renamed index gets a key that stands for every restriction in the access control. */
    public void testWillWriteCacheKeyForEveryDlsOrFlsWhenARenamedIndexHasNoEntryUnderEitherName() throws IOException {
        when(shardSearchRequest.shardId()).thenReturn(new ShardId("old-name", randomAlphaOfLength(10), 0));
        differentiatorWithCurrentName("renamed-name").accept(shardSearchRequest, out);
        assertThat(out.position(), greaterThan(0L));
    }

    /** An index that was not renamed is not covered by the access control, as it always was: nothing is written. */
    public void testWillDoNothingForAnIndexThatWasNotRenamedAndIsNotInTheAccessControl() throws IOException {
        when(shardSearchRequest.shardId()).thenReturn(new ShardId(indexName, randomAlphaOfLength(10), 0));
        differentiatorWithCurrentName(indexName).accept(shardSearchRequest, out);
        assertThat(out.position(), equalTo(0L));
    }
}
