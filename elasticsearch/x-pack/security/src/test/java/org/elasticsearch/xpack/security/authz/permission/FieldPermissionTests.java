/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class FieldPermissionTests extends ESTestCase {

    public void testParseFieldPermissions() throws Exception {
        String q = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "\"grant\": [\"f1\", \"f2\", \"f3\", \"f4\"]," +
                "\"except\": [\"f3\",\"f4\"]" +
                "}}]}";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(q), false);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getGrantedFieldsArray(),
                new String[]{"f1", "f2", "f3", "f4"});
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getDeniedFieldsArray(), new String[]{"f3", "f4"});

        q = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "\"except\": [\"f3\",\"f4\"]," +
                "\"grant\": [\"f1\", \"f2\", \"f3\", \"f4\"]" +
                "}}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getGrantedFieldsArray(),
                new String[]{"f1", "f2", "f3", "f4"});
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getDeniedFieldsArray(), new String[]{"f3", "f4"});

        q = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "\"grant\": [\"f1\", \"f2\"]" +
                "}}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getGrantedFieldsArray(), new String[]{"f1", "f2"});
        assertNull(rd.getIndicesPrivileges()[0].getFieldPermissions().getDeniedFieldsArray());

        q = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "\"grant\": []" +
                "}}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getGrantedFieldsArray(), new String[]{});
        assertNull(rd.getIndicesPrivileges()[0].getFieldPermissions().getDeniedFieldsArray());

        q = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "\"except\": []," +
                "\"grant\": []" +
                "}}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getGrantedFieldsArray(), new String[]{});
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getDeniedFieldsArray(), new String[]{});

        final String exceptWithoutGrant = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "\"except\": [\"f1\"]" +
                "}}]}";
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> RoleDescriptor.parse("test", new BytesArray
                (exceptWithoutGrant), false));
        assertThat(e.getDetailedMessage(), containsString("failed to parse indices privileges for role [test]. field_security requires " +
                "grant if except is given"));

        final String grantNull = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "\"grant\": null" +
                "}}]}";
        e = expectThrows(ElasticsearchParseException.class, () -> RoleDescriptor.parse("test", new BytesArray
                (grantNull), false));
        assertThat(e.getDetailedMessage(), containsString("failed to parse indices privileges for role [test]. grant must not be null."));

        final String exceptNull = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "\"grant\": [\"*\"]," +
                "\"except\": null" +
                "}}]}";
        e = expectThrows(ElasticsearchParseException.class, () -> RoleDescriptor.parse("test", new BytesArray
                (exceptNull), false));
        assertThat(e.getDetailedMessage(), containsString("failed to parse indices privileges for role [test]. except must not be null."));

        final String exceptGrantNull = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "\"grant\": null," +
                "\"except\": null" +
                "}}]}";
        e = expectThrows(ElasticsearchParseException.class, () -> RoleDescriptor.parse("test", new BytesArray
                (exceptGrantNull), false));
        assertThat(e.getDetailedMessage(), containsString("failed to parse indices privileges for role [test]. grant must not be null."));

        final String bothFieldsMissing = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "}}]}";
        e = expectThrows(ElasticsearchParseException.class, () -> RoleDescriptor.parse("test", new BytesArray
                (bothFieldsMissing), false));
        assertThat(e.getDetailedMessage(), containsString("failed to parse indices privileges for role [test]. \"field_security\" " +
                "must not be empty."));

        // try with two indices and mix order a little
        q = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"field_security\": {" +
                "\"grant\": []" +
                "}}," +
                "{\"names\": \"idx3\",\n" +
                " \"field_security\": {\n" +
                " \"grant\": [\"*\"], \n" +
                " \"except\": [\"f2\"]}," +
                "\"privileges\": [\"p3\"]}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getGrantedFieldsArray(), new String[]{});
        assertNull(rd.getIndicesPrivileges()[0].getFieldPermissions().getDeniedFieldsArray());
        assertArrayEquals(rd.getIndicesPrivileges()[1].getFieldPermissions().getGrantedFieldsArray(), new String[]{"*"});
        assertArrayEquals(rd.getIndicesPrivileges()[1].getFieldPermissions().getDeniedFieldsArray(), new String[]{"f2"});
    }

    // test old syntax for field permissions
    public void testBWCFieldPermissions() throws Exception {
        String q = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"fields\": [\"f1\", \"f2\"]" +
                "}]}";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(q), true);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getGrantedFieldsArray(), new String[]{"f1", "f2"});
        assertNull(rd.getIndicesPrivileges()[0].getFieldPermissions().getDeniedFieldsArray());

        final String failingQuery = q;
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> RoleDescriptor.parse("test", new BytesArray
                (failingQuery), false));
        assertThat(e.getDetailedMessage(), containsString("[\"fields\": [...]] format has changed for field permissions in role [test]" +
                ", use [\"field_security\": {\"grant\":[...],\"except\":[...]}] instead"));

        q = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"fields\": []" +
                "}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), true);
        assertArrayEquals(rd.getIndicesPrivileges()[0].getFieldPermissions().getGrantedFieldsArray(), new String[]{});
        assertNull(rd.getIndicesPrivileges()[0].getFieldPermissions().getDeniedFieldsArray());
        final String failingQuery2 = q;
        e = expectThrows(ElasticsearchParseException.class, () -> RoleDescriptor.parse("test", new BytesArray
                (failingQuery2), false));
        assertThat(e.getDetailedMessage(), containsString("[\"fields\": [...]] format has changed for field permissions in role [test]" +
                ", use [\"field_security\": {\"grant\":[...],\"except\":[...]}] instead"));

        q = "{\"indices\": [ {\"names\": \"idx2\", \"privileges\": [\"p3\"], " +
                "\"fields\": null" +
                "}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), true);
        assertNull(rd.getIndicesPrivileges()[0].getFieldPermissions().getGrantedFieldsArray());
        assertNull(rd.getIndicesPrivileges()[0].getFieldPermissions().getDeniedFieldsArray());
        final String failingQuery3 = q;
        e = expectThrows(ElasticsearchParseException.class, () -> RoleDescriptor.parse("test", new BytesArray(failingQuery3), false));
        assertThat(e.getDetailedMessage(), containsString("[\"fields\": [...]] format has changed for field permissions in role [test]" +
                ", use [\"field_security\": {\"grant\":[...],\"except\":[...]}] instead"));
    }

    public void testMergeFieldPermissions() {
        String allowedPrefix1 = randomAsciiOfLength(5);
        String allowedPrefix2 = randomAsciiOfLength(5);
        String[] allowed1 = new String[]{allowedPrefix1 + "*"};
        String[] allowed2 = new String[]{allowedPrefix2 + "*"};
        String[] denied1 = new String[]{allowedPrefix1 + "a"};
        String[] denied2 = new String[]{allowedPrefix2 + "a"};
        FieldPermissions fieldPermissions1 = new FieldPermissions(allowed1, denied1);
        FieldPermissions fieldPermissions2 = new FieldPermissions(allowed2, denied2);
        FieldPermissions mergedFieldPermissions = FieldPermissions.merge(fieldPermissions1, fieldPermissions2);
        assertTrue(mergedFieldPermissions.grantsAccessTo(allowedPrefix1 + "b"));
        assertTrue(mergedFieldPermissions.grantsAccessTo(allowedPrefix2 + "b"));
        assertFalse(mergedFieldPermissions.grantsAccessTo(denied1[0]));
        assertFalse(mergedFieldPermissions.grantsAccessTo(denied2[0]));

        allowed1 = new String[]{randomAsciiOfLength(5) + "*", randomAsciiOfLength(5) + "*"};
        allowed2 = null;
        denied1 = new String[]{allowed1[0] + "a", allowed1[1] + "a"};
        denied2 = null;
        fieldPermissions1 = new FieldPermissions(allowed1, denied1);
        fieldPermissions2 = new FieldPermissions(allowed2, denied2);
        mergedFieldPermissions = FieldPermissions.merge(fieldPermissions1, fieldPermissions2);
        assertFalse(mergedFieldPermissions.hasFieldLevelSecurity());

        allowed1 = new String[]{};
        allowed2 = new String[]{randomAsciiOfLength(5) + "*", randomAsciiOfLength(5) + "*"};
        denied1 = new String[]{};
        denied2 = new String[]{allowed2[0] + "a", allowed2[1] + "a"};
        fieldPermissions1 = new FieldPermissions(allowed1, denied1);
        fieldPermissions2 = new FieldPermissions(allowed2, denied2);
        mergedFieldPermissions = FieldPermissions.merge(fieldPermissions1, fieldPermissions2);
        for (String field : allowed2) {
            assertTrue(mergedFieldPermissions.grantsAccessTo(field));
        }
        for (String field : denied2) {
            assertFalse(mergedFieldPermissions.grantsAccessTo(field));
        }

        allowed1 = randomBoolean() ? null : new String[]{"*"};
        allowed2 = randomBoolean() ? null : new String[]{"*"};
        denied1 = new String[]{"a"};
        denied2 = new String[]{"b"};
        fieldPermissions1 = new FieldPermissions(allowed1, denied1);
        fieldPermissions2 = new FieldPermissions(allowed2, denied2);
        mergedFieldPermissions = FieldPermissions.merge(fieldPermissions1, fieldPermissions2);
        assertTrue(mergedFieldPermissions.grantsAccessTo("a"));
        assertTrue(mergedFieldPermissions.grantsAccessTo("b"));

        // test merge does not remove _all
        allowed1 = new String[]{"_all"};
        allowed2 = new String[]{};
        denied1 = null;
        denied2 = null;
        fieldPermissions1 = new FieldPermissions(allowed1, denied1);
        assertTrue(fieldPermissions1.allFieldIsAllowed);
        fieldPermissions2 = new FieldPermissions(allowed2, denied2);
        assertFalse(fieldPermissions2.allFieldIsAllowed);
        mergedFieldPermissions = FieldPermissions.merge(fieldPermissions1, fieldPermissions2);
        assertTrue(mergedFieldPermissions.grantsAccessTo("_all"));
        assertTrue(mergedFieldPermissions.allFieldIsAllowed);
    }

    public void testFieldPermissionsStreaming() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        String[] allowed = new String[]{randomAsciiOfLength(5) + "*", randomAsciiOfLength(5) + "*", randomAsciiOfLength(5) + "*"};
        String[] denied = new String[]{allowed[0] + randomAsciiOfLength(5), allowed[1] + randomAsciiOfLength(5),
                allowed[2] + randomAsciiOfLength(5)};
        FieldPermissions fieldPermissions = new FieldPermissions(allowed, denied);
        out.writeOptionalWriteable(fieldPermissions);
        out.close();
        StreamInput in = out.bytes().streamInput();
        FieldPermissions readFieldPermissions = in.readOptionalWriteable(FieldPermissions::new);
        // order should be preserved in any case
        assertEquals(readFieldPermissions, fieldPermissions);
    }
}
