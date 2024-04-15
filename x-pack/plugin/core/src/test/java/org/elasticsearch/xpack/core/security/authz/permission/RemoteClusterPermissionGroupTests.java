/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

//TODO: more tests
public class RemoteClusterPermissionGroupTests extends AbstractXContentSerializingTestCase<RemoteClusterPermissionGroup> {

    @Override
    protected Writeable.Reader<RemoteClusterPermissionGroup> instanceReader() {
        return RemoteClusterPermissionGroup::new;
    }

    @Override
    protected RemoteClusterPermissionGroup createTestInstance() {
        return new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "*" });
    }

    @Override
    protected RemoteClusterPermissionGroup mutateInstance(RemoteClusterPermissionGroup instance) throws IOException {
        if (randomBoolean()) {
            return new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "foo", "bar" });
        } else {
            return new RemoteClusterPermissionGroup(new String[] { "foobar" }, new String[] { "*" });
        }
    }

    @Override
    protected RemoteClusterPermissionGroup doParseInstance(XContentParser parser) throws IOException {
        // fromXContent/parsing isn't supported since we still do old school manual parsing of the role descriptor
        return createTestInstance();
    }

}
