/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class TransportValidateQueryActionTests extends ESSingleNodeTestCase {

    /*
     * This test covers a fallthrough bug that we had, where if the index we were validating against did not exist, we would invoke the
     * failure listener, and then fallthrough and invoke the success listener too. This would cause problems when the listener was
     * ultimately wrapping sending a response on the channel, as it could lead to us sending both a failure or success responses, and having
     * them garbled together, or trying to write one after the channel had closed, etc.
     */
    public void testListenerOnlyInvokedOnceWhenIndexDoesNotExist() {
        assertThat(
            safeAwaitFailure(
                ValidateQueryResponse.class,
                listener -> client().admin()
                    .indices()
                    .validateQuery(new ValidateQueryRequest("non-existent-index"), ActionListener.assertOnce(listener))
            ),
            instanceOf(IndexNotFoundException.class)
        );
    }

    public void testNoSliceDefaultsToAllWhenSliceEnabledIndex() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createIndex("slice-enabled", Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build());

        ValidateQueryResponse response = safeAwait(
            listener -> client().admin().indices().validateQuery(new ValidateQueryRequest("slice-enabled"), listener)
        );
        assertTrue(response.isValid());
    }

    public void testRoutingRejectedWhenSliceEnabledIndex() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createIndex("slice-enabled", Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build());

        ValidateQueryRequest request = new ValidateQueryRequest("slice-enabled").routing("manual");
        Exception failure = safeAwaitFailure(
            ValidateQueryResponse.class,
            listener -> client().admin().indices().validateQuery(request, listener)
        );
        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertThat(failure.getMessage(), containsString("[routing] is not allowed when [index.slice.enabled] is true"));
    }

    public void testSliceRejectedWhenSliceDisabledIndex() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createIndex("slice-disabled");

        ValidateQueryRequest request = new ValidateQueryRequest("slice-disabled").searchSlice("s1");
        Exception failure = safeAwaitFailure(
            ValidateQueryResponse.class,
            listener -> client().admin().indices().validateQuery(request, listener)
        );
        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertThat(failure.getMessage(), containsString("[_slice] is not allowed when [index.slice.enabled] is false"));
    }

    public void testSliceAcceptedWhenSliceEnabledIndex() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createIndex("slice-enabled", Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build());

        ValidateQueryRequest request = new ValidateQueryRequest("slice-enabled").searchSlice("s1");
        ValidateQueryResponse response = safeAwait(listener -> client().admin().indices().validateQuery(request, listener));
        assertTrue(response.isValid());
    }

}
