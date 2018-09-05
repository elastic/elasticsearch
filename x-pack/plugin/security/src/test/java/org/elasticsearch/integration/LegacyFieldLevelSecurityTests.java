/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.integration;

import org.elasticsearch.Version;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class LegacyFieldLevelSecurityTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = new SecureString("change_me".toCharArray());

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateSecurity.class, CommonAnalysisPlugin.class, ParentJoinPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected String configUsers() {
        final String usersPasswHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers() +
                "user1:" + usersPasswHashed + "\n" +
                "user2:" + usersPasswHashed + "\n" +
                "user3:" + usersPasswHashed + "\n" +
                "user4:" + usersPasswHashed + "\n" +
                "user5:" + usersPasswHashed + "\n" +
                "user6:" + usersPasswHashed + "\n" +
                "user7:" + usersPasswHashed + "\n" +
                "user8:" + usersPasswHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "role1:user1\n" +
                "role2:user1,user7,user8\n" +
                "role3:user2,user7,user8\n" +
                "role4:user3,user7\n" +
                "role5:user4,user7\n" +
                "role6:user5,user7\n" +
                "role7:user6";
    }
    @Override
    protected String configRoles() {
        return super.configRoles() +
                "\nrole1:\n" +
                "  cluster: [ none ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ none ]\n" +
                "role2:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "      - names: '*'\n" +
                "        privileges: [ ALL ]\n" +
                "        field_security:\n" +
                "           grant: [ field1, join_field* ]\n" +
                "role3:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "      - names: '*'\n" +
                "        privileges: [ ALL ]\n" +
                "        field_security:\n" +
                "           grant: [ field2, query* ]\n" +
                "role4:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "     - names: '*'\n" +
                "       privileges: [ ALL ]\n" +
                "       field_security:\n" +
                "           grant: [ field1, field2]\n" +
                "role5:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "      - names: '*'\n" +
                "        privileges: [ ALL ]\n" +
                "        field_security:\n" +
                "           grant: [ ]\n" +
                "role6:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "     - names: '*'\n" +
                "       privileges: [ALL]\n" +
                "role7:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "      - names: '*'\n" +
                "        privileges: [ ALL ]\n" +
                "        field_security:\n" +
                "           grant: [ 'field*' ]\n";
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
                .build();
    }

    public void testParentChild_parentField() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("_parent")
                .field("type", "parent")
                .endObject()
                .startObject("properties")
                .startObject("field1")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_6_0.id))
                .addMapping("parent")
                .addMapping("child", mapping));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("test", "child", "c1").setSource("field1", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("field1", "yellow").setParent("p1").get();
        refresh();
        FieldLevelSecurityTests.verifyParentChild();
    }

}
