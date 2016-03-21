/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.ShieldTestsUtils.assertAuthorizationException;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

public class IndexAliasesTests extends ShieldIntegTestCase {

    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(new SecuredString("test123".toCharArray())));

    @Override
    protected String configUsers() {
        return super.configUsers() +
                "create_only:" + USERS_PASSWD_HASHED + "\n" +
                "create_test_aliases_test:" + USERS_PASSWD_HASHED + "\n" +
                "create_test_aliases_alias:" + USERS_PASSWD_HASHED + "\n" +
                "create_test_aliases_test_alias:" + USERS_PASSWD_HASHED + "\n" +
                "aliases_only:" + USERS_PASSWD_HASHED + "\n";
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
                "    - names: '*'\n" +
                "      privileges: [ create_index ]\n" +
                //role that has create index and manage_aliases on test_*, not enough to manage_aliases aliases outside of test_* namespace
                "create_test_aliases_test:\n" +
                "  indices:\n" +
                "    - names: 'test_*'\n" +
                "      privileges: [ create_index, 'indices:admin/aliases*' ]\n" +
                //role that has create index on test_* and manage_aliases on alias_*, can't create aliases pointing to test_* though
                "create_test_aliases_alias:\n" +
                "  indices:\n" +
                "    - names: 'test_*'\n" +
                "      privileges: [ create_index ]\n" +
                "    - names: 'alias_*'\n" +
                "      privileges: [ 'indices:admin/aliases*' ]\n" +
                //role that has create index on test_* and manage_aliases on both alias_* and test_*
                "create_test_aliases_test_alias:\n" +
                "  indices:\n" +
                "    - names: 'test_*'\n" +
                "      privileges: [ create_index ]\n" +
                "    - names: [ 'alias_*', 'test_*' ]\n" +
                "      privileges: [ 'indices:admin/aliases*' ]\n" +
                //role that has manage_aliases only on both test_* and alias_*
                "aliases_only:\n" +
                "  indices:\n" +
                "    - names: [ 'alias_*', 'test_*']\n" +
                "      privileges: [ 'indices:admin/aliases*' ]\n";
    }

    @Before
    public void createBogusIndex() {
        if (randomBoolean()) {
            //randomly create an index with two aliases from user admin, to make sure it doesn't affect any of the test results
            assertAcked(client().admin().indices().prepareCreate("index1").addAlias(new Alias("alias1")).addAlias(new Alias("alias2")));
        }
    }

    public void testCreateIndexThenAliasesCreateOnlyPermission() {
        //user has create permission only: allows to create indices, manage_aliases is required to add/remove aliases
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only",
                new SecuredString("test123".toCharArray())));
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareCreate("test_1").get());

        try {
            client().filterWithHeader(headers).admin().indices().prepareAliases().addAlias("test_1", "test_alias").get();
            fail("add alias should have failed due to missing manage_aliases privileges");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:admin/aliases] is unauthorized for user [create_only]"));
        }

        try {
            client().filterWithHeader(headers).admin().indices().prepareAliases().addAlias("test_*", "test_alias").get();
            fail("add alias should have failed due to missing manage_aliases privileges");
        } catch(IndexNotFoundException e) {
             assertThat(e.toString(), containsString("[test_*]"));
        }
    }

    public void testCreateIndexAndAliasesCreateOnlyPermission() {
        //user has create permission only: allows to create indices, manage_aliases is required to add aliases although they are part of
        // the same create index request
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only",
                new SecuredString("test123".toCharArray())));
        try {
            client().filterWithHeader(headers).admin().indices().prepareCreate("test_1").addAlias(new Alias("test_2")).get();
            fail("create index should have failed due to missing manage_aliases privileges");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:admin/aliases] is unauthorized for user [create_only]"));
        }
    }

    public void testDeleteAliasesCreateOnlyPermission() {
        //user has create permission only: allows to create indices, manage_aliases is required to add/remove aliases
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only",
                new SecuredString("test123".toCharArray())));
        try {
            client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "alias_1").get();
            fail("remove alias should have failed due to missing manage_aliases privileges");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:admin/aliases] is unauthorized for user [create_only]"));
        }

        try {
            client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "alias_*").get();
            fail("remove alias should have failed due to missing manage_aliases privileges");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[alias_*"));
        }

        try {
            client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "_all").get();
            fail("remove alias should have failed due to missing manage_aliases privileges");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[_all]"));
        }
    }

    public void testGetAliasesCreateOnlyPermission() {
        //user has create permission only: allows to create indices, manage_aliases is required to retrieve aliases though
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only",
                new SecuredString("test123".toCharArray())));
        try {
            client().filterWithHeader(headers).admin().indices().prepareGetAliases("test_1").setIndices("test_1")
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
            fail("get alias should have failed due to missing manage_aliases privileges");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:admin/aliases/get] is unauthorized for user [create_only]"));
        }

        try {
            client().filterWithHeader(headers).admin().indices().prepareGetAliases("_all").setIndices("test_1")
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
            fail("get alias should have failed due to missing manage_aliases privileges");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[_all]"));
        }

        try {
            client().filterWithHeader(headers).admin().indices().prepareGetAliases().setIndices("test_1")
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
            fail("get alias should have failed due to missing manage_aliases privileges");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[_all]"));
        }

        try {
            client().filterWithHeader(headers).admin().indices().prepareGetAliases("test_alias").setIndices("test_*")
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
            fail("get alias should have failed due to missing manage_aliases privileges");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[test_*]"));
        }

        try {
            client().filterWithHeader(headers).admin().indices().prepareGetAliases().get();
            fail("get alias should have failed due to missing manage_aliases privileges");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[_all]"));
        }
    }

    public void testCreateIndexThenAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to add/remove aliases on both aliases and
        // indices
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray())));

        assertAcked(client().filterWithHeader(headers).admin().indices().prepareCreate("test_1").get());

        //ok: user has manage_aliases on test_*
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareAliases().addAlias("test_1", "test_alias").get());

        //ok: user has manage_aliases on test_*
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareAliases().addAlias("test_*", "test_alias_2").get());

        try {
            //fails: user doesn't have manage_aliases on alias_1
            client().filterWithHeader(headers).admin().indices().prepareAliases().addAlias("test_1", "alias_1")
                    .addAlias("test_1", "test_alias").get();
            fail("add alias should have failed due to missing manage_aliases privileges on alias_1");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_test]"));
        }
    }

    public void testCreateIndexAndAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to add/remove aliases on both aliases and
        // indices
        //ok: user has manage_aliases on test_*
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test",
                new SecuredString("test123".toCharArray())));
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).get());

        try {
            //fails: user doesn't have manage_aliases on alias_1
            client().filterWithHeader(headers).admin().indices().prepareCreate("test_2").addAlias(new Alias("test_alias"))
                    .addAlias(new Alias("alias_2")).get();
            fail("create index should have failed due to missing manage_aliases privileges on alias_2");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_test]"));
        }
    }

    public void testDeleteAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to add/remove aliases on both aliases and
        // indices
        //ok: user has manage_aliases on test_*
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test",
                new SecuredString("test123".toCharArray())));

        assertAcked(client().filterWithHeader(headers).admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias_1"))
                .addAlias(new Alias("test_alias_2"))
                .addAlias(new Alias("test_alias_3")).addAlias(new Alias("test_alias_4")).get());
        //ok: user has manage_aliases on test_*
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "test_alias_1").get());
        //ok: user has manage_aliases on test_*
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_*", "test_alias_2").get());
        //ok: user has manage_aliases on test_*
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "test_alias_*").get());

        try {
            //fails: all aliases have been deleted, no existing aliases match test_alias_*
            client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "test_alias_*").get();
            fail("remove alias should have failed due to no existing matching aliases to expand test_alias_* to");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[test_alias_*]"));
        }

        try {
            //fails: all aliases have been deleted, no existing aliases match _all
            client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "_all").get();
            fail("remove alias should have failed due to no existing matching aliases to expand _all to");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[_all]"));
        }

        try {
            //fails: user doesn't have manage_aliases on alias_1
            client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "alias_1").get();
            fail("remove alias should have failed due to missing manage_aliases privileges on alias_1");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_test]"));
        }

        try {
            //fails: user doesn't have manage_aliases on alias_1
            client().filterWithHeader(headers).admin().indices().prepareAliases()
                    .removeAlias("test_1", new String[]{"_all", "alias_1"}).get();
            fail("remove alias should have failed due to missing manage_aliases privileges on alias_1");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_test]"));
        }
    }

    public void testGetAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to retrieve aliases on both aliases and
        // indices
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test", new SecuredString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);
        assertAcked(client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).get());

        //ok: user has manage_aliases on test_*
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_alias").setIndices("test_1"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, test_* gets resolved to test_1
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_alias").setIndices("test_*"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, empty indices gets resolved to _all indices (thus test_1)
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_alias"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, _all aliases gets resolved to test_alias and empty indices gets resolved to  _all
        // indices (thus test_1)
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("_all").setIndices("test_1"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, empty aliases gets resolved to test_alias and empty indices gets resolved to  _all
        // indices (thus test_1)
        assertAliases(client.admin().indices().prepareGetAliases().setIndices("test_1"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, test_* aliases gets resolved to test_alias and empty indices gets resolved to  _all
        // indices (thus test_1)
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_*").setIndices("test_1"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, _all aliases gets resolved to test_alias and _all indices becomes test_1
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("_all").setIndices("_all"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, empty aliases gets resolved to test_alias and empty indices becomes test_1
        assertAliases(client.admin().indices().prepareGetAliases(),
                "test_1", "test_alias");

        try {
            //fails: user has manage_aliases on test_*, although _all aliases and empty indices can be resolved, the explicit non
            // authorized alias (alias_1) causes the request to fail
            client.admin().indices().prepareGetAliases().setAliases("_all", "alias_1").get();
            fail("get alias should have failed due to missing manage_aliases privileges on alias_1");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases/get] is unauthorized for user [create_test_aliases_test]"));
        }

        try {
            //fails: user doesn't have manage_aliases on alias_1
            client.admin().indices().prepareGetAliases().setAliases("alias_1").get();
            fail("get alias should have failed due to missing manage_aliases privileges on alias_1");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases/get] is unauthorized for user [create_test_aliases_test]"));
        }
    }

    public void testCreateIndexThenAliasesCreateAndAliasesPermission2() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to add/remove aliases
        // on both aliases and indices
        assertAcked(client.admin().indices().prepareCreate("test_1"));

        try {
            //fails: user doesn't have manage_aliases aliases on test_1
            client.admin().indices().prepareAliases().addAlias("test_1", "test_alias").get();
            fail("add alias should have failed due to missing manage_aliases privileges on test_alias and test_1");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage_aliases aliases on test_1
            client.admin().indices().prepareAliases().addAlias("test_1", "alias_1").get();
            fail("add alias should have failed due to missing manage_aliases privileges on test_1");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage_aliases aliases on test_*, no matching indices to replace wildcards
            client.admin().indices().prepareAliases().addAlias("test_*", "alias_1").get();
            fail("add alias should have failed due to missing manage_aliases privileges on test_1");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[test_*]"));
        }
    }

    public void testCreateIndexAndAliasesCreateAndAliasesPermission2() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_alias", new
                SecuredString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to add/remove aliases
        // on both aliases and indices
        try {
            //fails: user doesn't have manage_aliases on test_1, create index is rejected as a whole
            client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).get();
            fail("create index should have failed due to missing manage_aliases privileges on test_1 and test_alias");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage_aliases on test_*, create index is rejected as a whole
            client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).addAlias(new Alias("alias_1")).get();
            fail("create index should have failed due to missing manage_aliases privileges on test_1 and test_alias");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }
    }

    public void testDeleteAliasesCreateAndAliasesPermission2() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to add/remove aliases
        // on both aliases and indices
        try {
            //fails: user doesn't have manage_aliases on test_1
            client.admin().indices().prepareAliases().removeAlias("test_1", "test_alias").get();
            fail("remove alias should have failed due to missing manage_aliases privileges on test_alias and test_1");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage_aliases on test_1
            client.admin().indices().prepareAliases().removeAlias("test_1", "alias_1").get();
            fail("remove alias should have failed due to missing manage_aliases privileges on test_1");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage_aliases on test_*, wildcards can't get replaced
            client.admin().indices().prepareAliases().removeAlias("test_*", "alias_1").get();
            fail("remove alias should have failed due to missing manage_aliases privileges on test_*");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[test_*]"));
        }
    }

    public void testGetAliasesCreateAndAliasesPermission2() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_alias", new SecuredString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to retrieve aliases
        // on both aliases and indices
        assertAcked(client.admin().indices().prepareCreate("test_1"));

        try {
            //fails: user doesn't have manage_aliases aliases on test_1, nor test_alias
            client.admin().indices().prepareGetAliases().setAliases("test_alias").setIndices("test_1").get();
            fail("get alias should have failed due to missing manage_aliases privileges on test_alias and test_1");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases/get] is unauthorized for user [create_test_aliases_alias]"));
        }

        try {
            //fails: user doesn't have manage_aliases aliases on test_*, no matching indices to replace wildcards
            client.admin().indices().prepareGetAliases().setIndices("test_*").setAliases("test_alias").get();
            fail("get alias should have failed due to missing manage_aliases privileges on test_*");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[test_*]"));
        }

        try {
            //fails: no existing indices to replace empty indices (thus _all)
            client.admin().indices().prepareGetAliases().setAliases("test_alias").get();
            fail("get alias should have failed due to missing manage_aliases privileges on any index");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[_all]"));
        }

        try {
            //fails: no existing aliases to replace wildcards
            client.admin().indices().prepareGetAliases().setIndices("test_1").setAliases("test_*").get();
            fail("get alias should have failed due to missing manage_aliases privileges on test_1");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[test_*]"));
        }

        try {
            //fails: no existing aliases to replace _all
            client.admin().indices().prepareGetAliases().setIndices("test_1").setAliases("_all").get();
            fail("get alias should have failed due to missing manage_aliases privileges on test_1");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[_all]"));
        }

        try {
            //fails: no existing aliases to replace empty aliases
            client.admin().indices().prepareGetAliases().setIndices("test_1").get();
            fail("get alias should have failed due to missing manage_aliases privileges on test_1");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[_all]"));
        }

        try {
            //fails: no existing aliases to replace empty aliases
            client.admin().indices().prepareGetAliases().get();
            fail("get alias should have failed due to missing manage_aliases privileges on test_1");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[_all]"));
        }
    }

    public void testCreateIndexThenAliasesCreateAndAliasesPermission3() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client.admin().indices().prepareCreate("test_1"));

        assertAcked(client.admin().indices().prepareAliases().addAlias("test_1", "test_alias"));

        assertAcked(client.admin().indices().prepareAliases().addAlias("test_1", "alias_1"));

        assertAcked(client.admin().indices().prepareAliases().addAlias("test_*", "alias_2"));
    }

    public void testCreateIndexAndAliasesCreateAndAliasesPermission3() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")));

        assertAcked(client.admin().indices().prepareCreate("test_2").addAlias(new Alias("test_alias_2")).addAlias(new Alias("alias_2")));
    }

    public void testDeleteAliasesCreateAndAliasesPermission3() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).addAlias(new Alias("alias_1"))
                .addAlias(new Alias("alias_2")).addAlias(new Alias("alias_3")));

        try {
            //fails: user doesn't have manage_aliases privilege on non_authorized
            client.admin().indices().prepareAliases().removeAlias("test_1", "non_authorized").removeAlias("test_1", "test_alias").get();
            fail("remove alias should have failed due to missing manage_aliases privileges on non_authorized");
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:admin/aliases] is unauthorized for user [create_test_aliases_test_alias]"));
        }

        assertAcked(client.admin().indices().prepareAliases().removeAlias("test_1", "alias_1"));

        assertAcked(client.admin().indices().prepareAliases().removeAlias("test_*", "_all"));

        try {
            //fails: all aliases have been deleted, _all can't be resolved to any existing authorized aliases
            client.admin().indices().prepareAliases().removeAlias("test_1", "_all").get();
            fail("remove alias should have failed due to no existing aliases matching _all");
        } catch(IndexNotFoundException e) {
            assertThat(e.toString(), containsString("[_all]"));
        }
    }

    public void testGetAliasesCreateAndAliasesPermission3() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test_alias", new SecuredString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).addAlias(new Alias("alias_1")));

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_alias").setIndices("test_1"),
                "test_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("alias_1").setIndices("test_1"),
                "test_1", "alias_1");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("alias_1").setIndices("test_*"),
                "test_1", "alias_1");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_*").setIndices("test_1"),
                "test_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("_all").setIndices("test_1"),
                "test_1", "alias_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("_all"),
                "test_1", "alias_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases().setIndices("test_1"),
                "test_1", "alias_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases(), "test_1", "alias_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("alias_*").setIndices("test_*"),
                "test_1", "alias_1");
    }

    public void testCreateIndexAliasesOnlyPermission() {
        try {
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER,
                    basicAuthHeaderValue("aliases_only", new SecuredString("test123".toCharArray()))))
                    .admin().indices().prepareCreate("test_1").get();
            fail("Expected ElasticsearchSecurityException");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), is("action [indices:admin/create] is unauthorized for user [aliases_only]"));
        }
    }

    public void testGetAliasesAliasesOnlyPermission() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("aliases_only", new SecuredString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);
        //user has manage_aliases only permissions on both alias_* and test_*

        //ok: manage_aliases on both test_* and alias_*
        GetAliasesResponse getAliasesResponse = client.admin().indices().prepareGetAliases("alias_1")
                .addIndices("test_1").setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        assertThat(getAliasesResponse.getAliases().isEmpty(), is(true));

        try {
            //fails: no manage_aliases privilege on non_authorized alias
            client.admin().indices().prepareGetAliases("non_authorized").addIndices("test_1")
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:admin/aliases/get] is unauthorized for user [aliases_only]"));
        }

        try {
            //fails: no manage_aliases privilege on non_authorized index
            client.admin().indices().prepareGetAliases("alias_1").addIndices("non_authorized")
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:admin/aliases/get] is unauthorized for user [aliases_only]"));
        }
    }

    private static void assertAliases(GetAliasesRequestBuilder getAliasesRequestBuilder, String index, String... aliases) {
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.get();
        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(index).size(), equalTo(aliases.length));
        for (int i = 0; i < aliases.length; i++) {
            assertThat(getAliasesResponse.getAliases().get(index).get(i).alias(), equalTo(aliases[i]));
        }
    }
}
