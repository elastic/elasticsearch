/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackClientPlugin;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class GetUserPrivilegesRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final String user = randomAlphaOfLengthBetween(3, 12);

        final GetUserPrivilegesRequest original = new GetUserPrivilegesRequest();
        original.username(user);

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin(Settings.EMPTY).getNamedWriteables());
        StreamInput in = new NamedWriteableAwareStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), registry);
        final GetUserPrivilegesRequest copy = new GetUserPrivilegesRequest(in);

        assertThat(copy.username(), equalTo(original.username()));
        assertThat(copy.usernames(), equalTo(original.usernames()));
    }

}
