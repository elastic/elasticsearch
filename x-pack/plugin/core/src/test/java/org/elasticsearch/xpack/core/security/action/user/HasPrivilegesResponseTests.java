/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class HasPrivilegesResponseTests extends ESTestCase {

    public void testSerializationV64OrLater() throws IOException {
        final HasPrivilegesResponse original = randomResponse();
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_4_0, Version.CURRENT);
        final HasPrivilegesResponse copy = serializeAndDeserialize(original, version);

        assertThat(copy.isCompleteMatch(), equalTo(original.isCompleteMatch()));
//        assertThat(copy.getClusterPrivileges(), equalTo(original.getClusterPrivileges()));
        assertThat(copy.getIndexPrivileges(), equalTo(original.getIndexPrivileges()));
        assertThat(copy.getApplicationPrivileges(), equalTo(original.getApplicationPrivileges()));
    }

    public void testSerializationV63() throws IOException {
        final HasPrivilegesResponse original = randomResponse();
        final HasPrivilegesResponse copy = serializeAndDeserialize(original, Version.V_6_3_0);

        assertThat(copy.isCompleteMatch(), equalTo(original.isCompleteMatch()));
//        assertThat(copy.getClusterPrivileges(), equalTo(original.getClusterPrivileges()));
        assertThat(copy.getIndexPrivileges(), equalTo(original.getIndexPrivileges()));
        assertThat(copy.getApplicationPrivileges(), equalTo(Collections.emptyMap()));
    }

    private HasPrivilegesResponse serializeAndDeserialize(HasPrivilegesResponse original, Version version) throws IOException {
        logger.info("Test serialize/deserialize with version {}", version);
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        original.writeTo(out);

        final HasPrivilegesResponse copy = new HasPrivilegesResponse();
        final StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        copy.readFrom(in);
        assertThat(in.read(), equalTo(-1));
        return copy;
    }

    private HasPrivilegesResponse randomResponse() {
        final Map<String, Boolean> cluster = new HashMap<>();
        for (String priv : randomArray(1, 6, String[]::new, () -> randomAlphaOfLengthBetween(3, 12))) {
            cluster.put(priv, randomBoolean());
        }
        final Collection<HasPrivilegesResponse.ResourcePrivileges> index = randomResourcePrivileges();
        final Map<String, Collection<HasPrivilegesResponse.ResourcePrivileges>> application = new HashMap<>();
        for (String app : randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 6).toLowerCase(Locale.ROOT))) {
            application.put(app, randomResourcePrivileges());
        }
        return new HasPrivilegesResponse(randomBoolean(), cluster, index, application);
    }

    private Collection<HasPrivilegesResponse.ResourcePrivileges> randomResourcePrivileges() {
        final Collection<HasPrivilegesResponse.ResourcePrivileges> list = new ArrayList<>();
        for (String resource : randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(2, 6))) {
            final Map<String, Boolean> privileges = new HashMap<>();
            for (String priv : randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8))) {
                privileges.put(priv, randomBoolean());
            }
            list.add(new HasPrivilegesResponse.ResourcePrivileges(resource, privileges));
        }
        return list;
    }

}
