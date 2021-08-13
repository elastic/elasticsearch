/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.user.privileges;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class IndicesPrivilegesTests extends AbstractXContentTestCase<IndicesPrivileges> {

    public static IndicesPrivileges createNewRandom(String query) {
        final IndicesPrivileges.Builder indicesPrivilegesBuilder = IndicesPrivileges.builder()
            .indices(generateRandomStringArray(4, 4, false, false))
            .privileges(randomSubsetOf(randomIntBetween(1, 4), Role.IndexPrivilegeName.ALL_ARRAY))
            .allowRestrictedIndices(randomBoolean())
            .query(query);
        if (randomBoolean()) {
            final List<String> fields = Arrays.asList(generateRandomStringArray(4, 4, false));
            indicesPrivilegesBuilder.grantedFields(fields);
            if (randomBoolean()) {
                indicesPrivilegesBuilder.deniedFields(randomSubsetOf(fields));
            }
        }
        return indicesPrivilegesBuilder.build();
    }

    public void testToXContentWithNullFieldSecurity() {
        final IndicesPrivileges privileges = IndicesPrivileges.builder().indices("abc").privileges("all")
                .allowRestrictedIndices(randomBoolean()).build();
        final String json = Strings.toString(privileges);
        assertThat(json, not(containsString("field_security")));
    }

    public void testToXContentWithEmptyFieldSecurity() {
        final IndicesPrivileges privileges = IndicesPrivileges.builder()
            .indices("abc")
            .privileges("all")
            .grantedFields(Collections.emptyList())
            .deniedFields(Collections.emptyList())
            .allowRestrictedIndices(randomBoolean())
            .build();
        final String json = Strings.toString(privileges);
        assertThat(json, containsString("field_security"));
        assertThat(json, containsString("\"field_security\":{\"grant\":[],\"except\":[]}"));
    }

    public void testToXContentWithDeniedFieldsOnly() {
        final IndicesPrivileges privileges = IndicesPrivileges.builder()
            .indices("abc")
            .privileges("all")
            .deniedFields("secret.*")
            .allowRestrictedIndices(randomBoolean())
            .build();
        final String json = Strings.toString(privileges);
        assertThat(json, containsString("field_security"));
        assertThat(json, containsString("\"field_security\":{\"grant\":[\"*\"],\"except\":[\"secret.*\"]}"));
    }

    @Override
    protected IndicesPrivileges createTestInstance() {
        return createNewRandom(
                randomBoolean() ? null : "{ " + randomAlphaOfLengthBetween(1, 4) + " : " + randomAlphaOfLengthBetween(1, 4) + " }");
    }

    @Override
    protected IndicesPrivileges doParseInstance(XContentParser parser) throws IOException {
        return IndicesPrivileges.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
