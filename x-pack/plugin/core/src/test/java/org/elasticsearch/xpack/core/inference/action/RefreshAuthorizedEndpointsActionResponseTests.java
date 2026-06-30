/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class RefreshAuthorizedEndpointsActionResponseTests extends AbstractBWCWireSerializationTestCase<
    RefreshAuthorizedEndpointsAction.Response> {

    private static final RefreshAuthorizedEndpointsAction.Response REFRESHED_RESPONSE = new RefreshAuthorizedEndpointsAction.Response(
        RefreshAuthorizedEndpointsAction.Response.Status.REFRESHED
    );
    private static final RefreshAuthorizedEndpointsAction.Response CCM_DISABLED_RESPONSE = new RefreshAuthorizedEndpointsAction.Response(
        RefreshAuthorizedEndpointsAction.Response.Status.CCM_DISABLED
    );

    public void testStatus_Refreshed() {
        assertThat(REFRESHED_RESPONSE.status(), is(RefreshAuthorizedEndpointsAction.Response.Status.REFRESHED));
    }

    public void testStatus_CcmDisabled() {
        assertThat(CCM_DISABLED_RESPONSE.status(), is(RefreshAuthorizedEndpointsAction.Response.Status.CCM_DISABLED));
    }

    @Override
    protected RefreshAuthorizedEndpointsAction.Response mutateInstanceForVersion(
        RefreshAuthorizedEndpointsAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<RefreshAuthorizedEndpointsAction.Response> instanceReader() {
        return RefreshAuthorizedEndpointsAction.Response::new;
    }

    @Override
    protected RefreshAuthorizedEndpointsAction.Response createTestInstance() {
        return new RefreshAuthorizedEndpointsAction.Response(randomFrom(RefreshAuthorizedEndpointsAction.Response.Status.values()));
    }

    @Override
    protected RefreshAuthorizedEndpointsAction.Response mutateInstance(RefreshAuthorizedEndpointsAction.Response instance)
        throws IOException {
        var other = instance.status() == RefreshAuthorizedEndpointsAction.Response.Status.REFRESHED
            ? RefreshAuthorizedEndpointsAction.Response.Status.CCM_DISABLED
            : RefreshAuthorizedEndpointsAction.Response.Status.REFRESHED;
        return new RefreshAuthorizedEndpointsAction.Response(other);
    }
}
