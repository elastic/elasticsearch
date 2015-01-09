/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;

@ClusterScope(scope = Scope.SUITE)
public class IndexAliasesTests extends ShieldIntegrationTest {

    @Override
    protected String configUsers() {
        return super.configUsers() +
                "create_only:{plain}test123\n" +
                "create_test_aliases_test:{plain}test123\n" +
                "create_test_aliases_alias:{plain}test123\n" +
                "create_test_aliases_test_alias:{plain}test123\n" +
                "aliases_only:{plain}test123\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "create_only:create_only\n" +
                "create_test_aliases_test:create_test_aliases_test\n" +
                "create_test_aliases_alias:create_test_aliases_alias\n" +
                "create_test_aliases_test_alias:create_test_aliases_test_alias\n" +
                "aliases_only:aliases_only\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" +
                //role that has create index only privileges
                "create_only:\n" +
                "  indices:\n" +
                "    '*': create_index\n" +
                //role that has create index and managa aliases on test_*, not enough to manage aliases outside of test_* namespace
                "create_test_aliases_test:\n" +
                "  indices:\n" +
                "    'test_*': create_index,manage_aliases\n" +
                //role that has create index on test_* and manage aliases on alias_*, can't create aliases pointing to test_* though
                "create_test_aliases_alias:\n" +
                "  indices:\n" +
                "    'test_*': create_index\n" +
                "    'alias_*': manage_aliases\n" +
                //role that has create index on test_* and manage_aliases on both alias_* and test_*
                "create_test_aliases_test_alias:\n" +
                "  indices:\n" +
                "    'test_*': create_index\n" +
                "    'alias_*,test_*': manage_aliases\n" +
                //role that has manage_aliases only on both test_* and alias_*
                "aliases_only:\n" +
                "  indices:\n" +
                "    'alias_*,test_*': manage_aliases\n";
    }

    @Test
    public void testCreateIndexThenAliasesCreateOnlyPermission() {
        //user has create permission only: allows to create indices, manage_aliases is required to add/remove aliases
        assertAcked(client().admin().indices().prepareCreate("test_1")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only", new SecuredString("test123".toCharArray()))));

        try {
            client().admin().indices().prepareAliases().addAlias("test_1", "test_alias")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only", new SecuredString("test123".toCharArray()))).get();
            fail("add alias should have failed due to missing manage_aliases privileges");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_only]"));
        }

        try {
            client().admin().indices().prepareAliases().addAlias("test_*", "test_alias")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only", new SecuredString("test123".toCharArray()))).get();
            fail("add alias should have failed due to missing manage_aliases privileges");
        } catch(IndexMissingException e) {
             assertThat(e.getMessage(), containsString("[test_*]"));
        }
    }

    @Test
    public void testCreateIndexAndAliasesCreateOnlyPermission() {
        //user has create permission only: allows to create indices, manage_aliases is required to add aliases although they are part of the same create index request
        try {
            client().admin().indices().prepareCreate("test_1").addAlias(new Alias("test_2"))
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only", new SecuredString("test123".toCharArray()))).get();
            fail("create index should have failed due to missing manage_aliases privileges");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_only]"));
        }
    }

    @Test
    public void testDeleteAliasesCreateOnlyPermission() {
        //user has create permission only: allows to create indices, manage_aliases is required to add/remove aliases
        try {
            client().admin().indices().prepareAliases().removeAlias("test_1", "alias_1")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to missing manage_aliases privileges");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_only]"));
        }

        try {
            client().admin().indices().prepareAliases().removeAlias("test_1", "alias_*")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to missing manage_aliases privileges");
        } catch(IndexMissingException e) {
            assertThat(e.getMessage(), containsString("[alias_*"));
        }

        try {
            client().admin().indices().prepareAliases().removeAlias("test_1", "_all")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to missing manage_aliases privileges");
        } catch(IndexMissingException e) {
            assertThat(e.getMessage(), containsString("[_all]"));
        }
    }

    @Test
    public void testCreateIndexThenAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to add/remove aliases on both aliases and indices
        assertAcked(client().admin().indices().prepareCreate("test_1")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))));

        //ok: user has manage_aliases on test_*
        assertAcked(client().admin().indices().prepareAliases().addAlias("test_1", "test_alias")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))));

        //ok: user has manage_aliases on test_*
        assertAcked(client().admin().indices().prepareAliases().addAlias("test_*", "test_alias_2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))));

        try {
            //fails: user doesn't have manage_aliases on alias_1
            client().admin().indices().prepareAliases().addAlias("test_1", "alias_1", "test_alias")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))).get();
            fail("add alias should have failed due to missing manage_aliases privileges on alias_1");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_test]"));
        }
    }

    @Test
    public void testCreateIndexAndAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to add/remove aliases on both aliases and indices
        //ok: user has manage_aliases on test_*
        assertAcked(client().admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias"))
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))));

        try {
            //fails: user doesn't have manage_aliases on alias_1
            client().admin().indices().prepareCreate("test_2").addAlias(new Alias("test_alias")).addAlias(new Alias("alias_2"))
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))).get();
            fail("create index should have failed due to missing manage_aliases privileges on alias_2");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_test]"));
        }
    }

    @Test
    public void testDeleteAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to add/remove aliases on both aliases and indices
        //ok: user has manage_aliases on test_*
        assertAcked(client().admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias_1")).addAlias(new Alias("test_alias_2"))
                .addAlias(new Alias("test_alias_3")).addAlias(new Alias("test_alias_4"))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))));
        //ok: user has manage_aliases on test_*
        assertAcked(client().admin().indices().prepareAliases().removeAlias("test_1", "test_alias_1")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))));
        //ok: user has manage_aliases on test_*
        assertAcked(client().admin().indices().prepareAliases().removeAlias("test_*", "test_alias_2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))));
        //ok: user has manage_aliases on test_*
        assertAcked(client().admin().indices().prepareAliases().removeAlias("test_1", "test_alias_*")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))));

        try {
            //fails: all aliases have been deleted, no existing aliases match test_alias_*
            client().admin().indices().prepareAliases().removeAlias("test_1", "test_alias_*")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to no existing matching aliases to expand test_alias_* to");
        } catch(IndexMissingException e) {
            assertThat(e.getMessage(), containsString("[test_alias_*]"));
        }

        try {
            //fails: all aliases have been deleted, no existing aliases match _all
            client().admin().indices().prepareAliases().removeAlias("test_1", "_all")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to no existing matching aliases to expand _all to");
        } catch(IndexMissingException e) {
            assertThat(e.getMessage(), containsString("[_all]"));
        }

        try {
            //fails: user doesn't have manage_aliases on alias_1
            client().admin().indices().prepareAliases().removeAlias("test_1", "alias_1")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to missing manage_aliases privileges on alias_1");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_test]"));
        }
    }

    @Test
    public void testCreateIndexThenAliasesCreateAndAliasesPermission2() {
        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to add/remove aliases on both aliases and indices
        assertAcked(client().admin().indices().prepareCreate("test_1")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray()))));

        try {
            //fails: user doesn't have manage aliases on test_1
            client().admin().indices().prepareAliases().addAlias("test_1", "test_alias")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray()))).get();
            fail("add alias should have failed due to missing manage_aliases privileges on test_alias and test_1");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage aliases on test_1
            client().admin().indices().prepareAliases().addAlias("test_1", "alias_1")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray()))).get();
            fail("add alias should have failed due to missing manage_aliases privileges on test_1");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage aliases on test_*, no matching indices to replace wildcards
            client().admin().indices().prepareAliases().addAlias("test_*", "alias_1")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray()))).get();
            fail("add alias should have failed due to missing manage_aliases privileges on test_1");
        } catch(IndexMissingException e) {
            assertThat(e.getMessage(), containsString("[test_*]"));
        }
    }

    @Test
    public void testCreateIndexAndAliasesCreateAndAliasesPermission2() {
        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to add/remove aliases on both aliases and indices
        try {
            //fails: user doesn't have manage_aliases on test_1, create index is rejected as a whole
            client().admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias"))
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray()))).get();
            fail("create index should have failed due to missing manage_aliases privileges on test_1 and test_alias");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage_aliases on test_*, create index is rejected as a whole
            client().admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).addAlias(new Alias("alias_1"))
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray()))).get();
            fail("create index should have failed due to missing manage_aliases privileges on test_1 and test_alias");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }
    }

    @Test
    public void testDeleteAliasesCreateAndAliasesPermission2() {
        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to add/remove aliases on both aliases and indices
        try {
            //fails: user doesn't have manage_aliases on test_1
            client().admin().indices().prepareAliases().removeAlias("test_1", "test_alias")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to missing manage_aliases privileges on test_alias and test_1");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage_aliases on test_1
            client().admin().indices().prepareAliases().removeAlias("test_1", "alias_1")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to missing manage_aliases privileges on test_1");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage_aliases on test_*, wildcards can't get replaced
            client().admin().indices().prepareAliases().removeAlias("test_*", "alias_1")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to missing manage_aliases privileges on test_*");
        } catch(IndexMissingException e) {
            assertThat(e.getMessage(), containsString("[test_*]"));
        }
    }

    @Test
    public void testCreateIndexThenAliasesCreateAndAliasesPermission3() {
        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client().admin().indices().prepareCreate("test_1")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))));

        assertAcked(client().admin().indices().prepareAliases().addAlias("test_1", "test_alias")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))));

        assertAcked(client().admin().indices().prepareAliases().addAlias("test_1", "alias_1")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))));

        assertAcked(client().admin().indices().prepareAliases().addAlias("test_*", "alias_2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))));
    }

    @Test
    public void testCreateIndexAndAliasesCreateAndAliasesPermission3() {
        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client().admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias"))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))));

        assertAcked(client().admin().indices().prepareCreate("test_2").addAlias(new Alias("test_alias_2")).addAlias(new Alias("alias_2"))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))));
    }

    @Test
    public void testDeleteAliasesCreateAndAliasesPermission3() {
        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client().admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).addAlias(new Alias("alias_1"))
                .addAlias(new Alias("alias_2")).addAlias(new Alias("alias_3"))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))));

        try {
            //fails: user doesn't have manage_aliases privilege on non_authorized
            client().admin().indices().prepareAliases().removeAlias("test_1", "non_authorized").removeAlias("test_1", "test_alias")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to missing manage_aliases privileges on non_authorized");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_test_alias]"));
        }

        assertAcked(client().admin().indices().prepareAliases().removeAlias("test_1", "alias_1")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))));

        assertAcked(client().admin().indices().prepareAliases().removeAlias("test_*", "_all")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))));


        try {
            //fails: all aliases have been deleted, _all can't be resolved to any existing authorized aliases
            client().admin().indices().prepareAliases().removeAlias("test_1", "_all")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray()))).get();
            fail("remove alias should have failed due to no existing aliases matching _all");
        } catch(IndexMissingException e) {
            assertThat(e.getMessage(), containsString("[_all]"));
        }
    }

    @Test(expected = AuthorizationException.class)
    public void testCreateIndexAliasesOnlyPermission() {
        client().admin().indices().prepareCreate("test_1")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("aliases_only", new SecuredString("test123".toCharArray()))).get();
    }
}
