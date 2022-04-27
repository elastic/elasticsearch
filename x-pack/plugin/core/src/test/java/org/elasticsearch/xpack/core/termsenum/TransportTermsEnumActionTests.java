/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransportTermsEnumActionTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class);
    }

    /*
     * Copy of test that tripped up similarly broadcast ValidateQuery
     */
    public void testListenerOnlyInvokedOnceWhenIndexDoesNotExist() {
        final AtomicBoolean invoked = new AtomicBoolean();
        final ActionListener<TermsEnumResponse> listener = new ActionListener<>() {

            @Override
            public void onResponse(final TermsEnumResponse validateQueryResponse) {
                fail("onResponse should not be invoked in this failure case");
            }

            @Override
            public void onFailure(final Exception e) {
                if (invoked.compareAndSet(false, true) == false) {
                    fail("onFailure invoked more than once");
                }
            }

        };
        client().execute(TermsEnumAction.INSTANCE, new TermsEnumRequest("non-existent-index"), listener);
        assertThat(invoked.get(), equalTo(true)); // ensure that onFailure was invoked
    }

    /**
     * Test that triggering the CCS compatibility check with a query that shouldn't go to the minor before Version.CURRENT works
     */
    public void testCCSCheckCompatibility() throws Exception {
        TermsEnumRequest request = new TermsEnumRequest().field("field").timeout(TimeValue.timeValueSeconds(5));
        request.indexFilter(new DummyQueryBuilder() {
            @Override
            public Version getMinimalSupportedVersion() {
                return Version.CURRENT;
            }
        });
        ExecutionException ex = expectThrows(ExecutionException.class, () -> client().execute(TermsEnumAction.INSTANCE, request).get());
        assertThat(ex.getCause().getMessage(), containsString("not compatible with version"));
        assertThat(ex.getCause().getMessage(), containsString("the 'search.check_ccs_compatibility' setting is enabled."));
        assertThat(
            ex.getCause().getCause().getMessage(),
            containsString(
                "was released first in version " + Version.CURRENT + ", failed compatibility check trying to send it to node with version"
            )
        );
    }
}
