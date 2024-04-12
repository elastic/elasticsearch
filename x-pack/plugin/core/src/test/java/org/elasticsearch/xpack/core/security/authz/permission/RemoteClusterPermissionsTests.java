/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

//TODO: more tests
public class RemoteClusterPermissionsTests extends AbstractXContentSerializingTestCase<RemoteClusterPermissions> {

    @Override
    protected Writeable.Reader<RemoteClusterPermissions> instanceReader() {
        return RemoteClusterPermissions::new;
    }

    @Override
    protected RemoteClusterPermissions createTestInstance() {
        return new RemoteClusterPermissions().addGroup(
            new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "*" })
        );
    }

    @Override
    protected RemoteClusterPermissions mutateInstance(RemoteClusterPermissions instance) throws IOException {
        return new RemoteClusterPermissions().addGroup(
            new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "*" })
        ).addGroup(new RemoteClusterPermissionGroup(new String[] { "foobar" }, new String[] { "*" }));
    }

    @Override
    protected RemoteClusterPermissions doParseInstance(XContentParser parser) throws IOException {
        // fromXContent/parsing isn't supported since we still do old school manual parsing of the role descriptor
        return createTestInstance();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    RemoteClusterPermissionGroup.class,
                    RemoteClusterPermissionGroup.NAME,
                    RemoteClusterPermissionGroup::new))
        );
    }
}
